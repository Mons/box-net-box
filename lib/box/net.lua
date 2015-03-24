local oop = require 'box.oop'
local clr = require 'devel.caller'

local NOTCONNECTED = 0;
local CONNECTING   = 1;
local CONNECTED    = 2;
local RECONNECTING = 3;

local M = oop:define({ __name = 'box.net' })
box.net = M

local S2S = {
	[NOTCONNECTED]  = 'NOTCONNECTED',
	[CONNECTING]    = 'CONNECTING',
	[CONNECTED]     = 'CONNECTED',
	[RECONNECTING]  = 'RECONNECTING',
}
M.S2S = S2S

function M:init(host, port, opt)
	self.host = host;
	self.port = tonumber(port)
	opt = opt or {}
	
	self.timeout = tonumber(opt.timeout) or 1/3
	
	if opt.autoconnect ~= nil then
		self._auto = opt.autoconnect
	else
		self._auto = true
	end
	
	if opt.reconnect ~= nil then
		if opt.reconnect then
			self._reconnect = tonumber(opt.reconnect)
		else
			self._reconnect = false
		end
	else
		self._reconnect = 1/3
	end
	
	self.maxbuf  = 256*1024
	-- self.maxbuf  = 32
	
	self.rbuf = ''
	self.connwait = setmetatable({},{__mode = "kv"})
	if opt.deadly or opt.deadly == nil then
		box.reload:register(self)
	end
end

function M:log(l,msg,...)
	msg = tostring(msg)
	if string.match(msg,'%%') then
		msg = string.format(msg,...)
	else
		for _,v in pairs({...}) do
			msg = msg .. ' ' .. tostring(v)
		end
	end
	print(string.format( "[%s] {%s:%s} %s", l, self.host, self.port, msg ))
end

function box.net:stringify()
	return string.format("cnn(%s:%s : %s:%s : %s)",self:fdno(),self.state,self.host,self.port,self.__id)
end

function M:on_connected()
	print("default connected callback")
end

function M:on_disconnect(e)
	print("default disconnected callback ",e)
end

--[[
function M:on_connfail(e)
	--print("connfail ",e)
end
]]

function M:_cleanup(e)
	--print('slef.ww')
	if self.ww then if self.ww ~= box.fiber.self() then pcall(box.fiber.cancel,self.ww) end self.ww = nil end
	--print('slef.rw ', self.rw)
	if self.rw then if self.rw ~= box.fiber.self() then pcall(box.fiber.cancel,self.rw) end self.rw = nil end
	--print('slef.s')
	if self.s  then self.s:close() self.s = nil end
	--print('slef.le')
	self.lasterror = box.errno.strerror(e)
	--print('slef.cw')
	for k,v in pairs(self.connwait) do
		--print('slef.cw put')
		k:put(false)
		--print('slef.cw nil')
		self.connwait[k] = nil
	end
	--print('_cleanup')
end

function M:destroy()
	self:_cleanup(0)
	local name = self.host..':'..self.port
	for k in pairs(self) do
		self[k] = nil
	end
	setmetatable(self,{
		__index = function(s,n)
			print("access to `"..n.."' on destroyed con "..name..clr(1))
			--print('index: ', box.fiber.id(box.fiber.self()))
			box.fiber.cancel(box.fiber.self())
			--local r,e = pcall(box.fiber.cancel,box.fiber.self())
			--if not r then
			--	box.raise(51,"access to `"..n.."' on destroyed con "..name..clr(1))
			--end
		end,
		__newindex = function(s,n)
			print("access to `"..n.."' on destroyed con"..name..clr(1))
			--print('newindex: ', box.fiber.id(box.fiber.self()))
			box.fiber.cancel(box.fiber.self())
			--local r,e = pcall(box.fiber.cancel,box.fiber.self())
			--if not r then
			--	box.raise(51,"access to `"..n.."' on destroyed con "..name..clr(1))
			--end
		end
	})
end

function M:close()
	self:_cleanup(0)
	print(self.host..':'..self.port..' closed')
--[[
	if self.s then
		self.s:close()
		self.s = nil
	end
--]]
end

function M:on_connect_failed(e)
	self:log('E','Connect failed:', box.errno.strerror(e))
	if self.state == nil or self.state == CONNECTING then
		self:log('W',"connect failed:",box.errno.strerror(e))
	end
	-- TODO: stop all fibers
	self:_cleanup(e)
	if self._reconnect then
		self.state = RECONNECTING
		if self.on_connfail then
			box.fiber.wrap(function(self) box.fiber.name("net.cb") self:on_connfail(box.errno.strerror(e)) end,self)
		end
		box.fiber.sleep(self._reconnect)
		self:connect()
	else
		self.state = NOTCONNECTED
		box.fiber.wrap(function(self) box.fiber.name("net.cb") self:on_disconnect(box.errno.strerror(e)) end,self)
	end
end

function M:on_connect_reset(e)
	self:log('W',"connection reset:",box.errno.strerror(e))
	-- TODO: stop all fibers
	self:_cleanup(e)
	
	if self._reconnect then
		self.state = NOTCONNECTED -- was RECONNECTING
		box.fiber.wrap(function(self) box.fiber.name("net.cb") self:on_disconnect(box.errno.strerror(e)) end,self)
		box.fiber.sleep(0)
		self:connect()
	else
		self.state = NOTCONNECTED
		box.fiber.wrap(function(self) box.fiber.name("net.cb") self:on_disconnect(box.errno.strerror(e)) end,self)
	end
end

function M:on_read(is_last)
	print("on_read (last:",is_last,") ",self.rbuf)
	self.rbuf = ''
end

function M:on_connect_io()
	local err = self.s:getsockopt('SOL_SOCKET', 'SO_ERROR');
	if err ~= 0 then
		-- TODO: error handling
		self:on_connect_failed( err )
		return
	end
	self.state = CONNECTED;
	--print("create reader")
	
	local weak = setmetatable({}, { __mode = "kv" })
	weak.self = self
	
	self.rw = box.fiber.wrap(function (weak)
		box.fiber.name("net.rw")
		local s = weak.self.s
		while true do
			box.fiber.testcancel()
			-- TODO: socket destroy
			local rd = s:readable()
			
			collectgarbage()
			if not weak.self then s:close() return end
			local self = weak.self
			
			if rd then
				--print("readable ")
				local rbuf = s:sysread( self.maxbuf - #self.rbuf )
				if rbuf then
					--print("received ",#rbuf)
					self.rbuf = self.rbuf .. rbuf
					local before = #self.rbuf
					
					self:on_read(#rbuf == 0)
					
					if #rbuf == 0 then
						self.rbuf = ''
						self.rw = nil
						self:on_connect_reset(box.errno.ECONNABORTED)
						return
					end
					
					if #self.rbuf == before and before == self.maxbuf then
						self.rbuf = ''
						self.rw = nil
						self:on_connect_reset(box.errno.ENOMEM)
						return
					end
					
				else
					self.rw = nil
					self:on_connect_reset(s:errno())
					return
				end
			else
				print("Error happens: ",s:error())
				self.rw = nil
				self:on_connect_reset(s:errno())
				return
			end
		end
	end,weak)
	for k,v in pairs(self.connwait) do
		k:put(true)
		self.connwait[k] = nil
	end
	box.fiber.wrap(function(self) box.fiber.name("net.cb") self:on_connected() end,self)
end

function M:connect()
	box.fiber.wrap(function()
	assert(type(self) == 'table',"object required")
	
	if self.state == NOTCONNECTED then
		self.state = CONNECTING;
	end
	-- connect timeout
	assert(not self.s, "Already have socket")
	
	local ai = box.socket.getaddrinfo( self.host, self.port, self.timeout, {
		['type'] = 'SOCK_STREAM',
	} )
	
	if ai and #ai > 0 then
		--print(dumper(ai))
	else
		self:on_connect_failed( box.errno() == 0 and box.errno.ENXIO or box.errno() )
		return
	end
	
	local ainfo = ai[1]
	local s = box.socket( ainfo.family, ainfo.type, ainfo.protocol )
	if not s then
		self:on_connect_failed( box.errno() )
		return
	end
	--print("created socket ",s, " ",s:nonblock())
	s:nonblock(true)
	s:linger(1,0)
	
	while true do
		if s:sysconnect( ainfo.host, ainfo.port ) then
			self.s = s
			--print("immediate connected")
			self:on_connect_io()
			return
		else
			if s:errno() == box.errno.EINPROGRESS
			or s:errno() == box.errno.EALREADY
			or s:errno() == box.errno.EWOULDBLOCK
			then
				self.s = s
				
				-- io/w watcher
				assert(not self.ww, "ww already set")
				
				local weak = setmetatable({}, { __mode = "kv" })
				weak.self = self
				
				self.ww =
				box.fiber.wrap(function(weak)
					box.fiber.name("net.cw")
					local wr = s:writable(weak.self.timeout)
					collectgarbage()
					if not weak.self then s:close() return end
					
					if wr then
						weak.self.ww = nil
						weak.self:on_connect_io()
						return
					else
						weak.self.ww = nil
						weak.self:on_connect_failed( box.errno.ETIMEDOUT )
						return
					end
				end,weak)
				return
			elseif s:errno() == box.errno.EINTR then
				-- again
			else
				self:on_connect_failed( s:errno() )
				return
			end
		end
	end
	
	end)
end

function M:write(buf)
	assert(type(self) == 'table',"object required")
	
	if self.state ~= CONNECTED then
		print("write in state ",S2S[self.state])
		if self._auto then
			if self.state == nil then
				self:connect()
			end
		end
		local connected = false
		if self.state ~= RECONNECTING then
			local ch = box.ipc.channel(1)
			self.connwait[ ch ] = ch
			connected = ch:get( self.timeout )
		end
		if not connected then
			box.fiber.sleep(0)
			box.raise(box.error.ER_PROC_LUA, "Not connected for write ("..tostring(self.state)..")")
		end
	end
	if self.wbuf then
		self.wbuf = self.wbuf .. buf
		return
	else
		--local wr = self.s:syswrite(buf:sub(1,#buf-1))
		if not self.s then
			print("Have no socket for write")
			box.fiber.cancel(box.fiber.self())
			return
		end
		local wr = self.s:syswrite(buf)
		if wr then
			--print("written ",wr," bytes of ",#buf)
			if wr == #buf then
				--print("written completely")
				return
			else
				--print("written partially")
				self.wbuf = buf:sub(wr+1)
				assert(not self.ww, "ww already set")
				
				local weak = setmetatable({}, { __mode = "kv" })
				weak.self = self
				
				self.ww = box.fiber.wrap(function (weak)
					box.fiber.name("net.ww")
					while true do
						box.fiber.testcancel()
						local wr = self.s:writable(self.timeout)
						
						collectgarbage()
						if not weak.self then s:close() return end
						local self = weak.self
						
						if wr then
							--print("ww")
							local wr = self.s:syswrite(self.wbuf)
							if wr then
								if wr == #self.wbuf then
									self.ww = nil
									self.wbuf = nil
									return
								else
									self.wbuf = self.wbuf:sub(wr+1)
								end
							else
								self:on_connect_reset( self.s:errno() )
								return
							end
						else
							print("writable failed: ",self.s:error())
							self:on_connect_reset( box.errno.ETIMEDOUT )
							return
						end
					end
				end,weak)
			end
		else
			self:on_connect_reset( self.s:errno() )
		end
	end
end


return M
