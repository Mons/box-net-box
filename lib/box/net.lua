local oop = require 'box.oop'
local clr = require 'devel.caller'
local ffi = require 'ffi'

if not pcall(ffi.typeof,"struct iovec") then
	ffi.cdef[[
		struct iovec {
			void  *iov_base;
			size_t iov_len;
		};
	]]
end

ffi.cdef [[
	char *strerror(int errnum);
	ssize_t read(int fd, void *buf, size_t count);	
	void *memcpy(void *dest, const void *src, size_t n);
	void *memmove(void *dest, const void *src, size_t n);
	ssize_t writev(int fd, const struct iovec *iov, int iovcnt);
]]

local C = ffi.C

local iovec = ffi.typeof('struct iovec')
local IOVSZ = ffi.sizeof(iovec)

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

local errno_is_transient = {
	[box.errno.EINPROGRESS] = true;
	[box.errno.EAGAIN] = true;
	[box.errno.EWOULDBLOCK] = true;
	[box.errno.EINTR] = true;
}


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
	
	self.maxbuf  = 2*1024*1024
	self.rbuf = ffi.new('char[?]', self.maxbuf)
	self.avail = 0ULL

	self.wsize = 32
	local osz = self.wsize
	self.wbuf = ffi.new('struct iovec[?]', self.wsize)
	-- ffi.gc(self.wbuf,function ( ... )
	-- 	print("gc'ed initial wbuf of size ", osz)
	-- end)

	self.wcur = 0
	self.wstash = {}

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
function box.net:fdno()
	local s = tostring(self.s)
	local x = { string.match(s,"fd%s+(-?%d+)") }
	if x[1] ~= nil then
		return tonumber(x[1])
	else
		return -1
	end
end

function box.net:stringify()
	return string.format("cnn(%s:%s : %s:%s : %s)",self:fdno(),self.state,self.host,self.port,self.__id)
end

function box.net:desc()
	return tostring(self.host) .. ':' .. tostring(self.port) .. '/' .. self:fdno()
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

	self.wcur   = 0
	self.wstash = {}
	self.avail  = 0ULL

	self.lasterror = box.errno.strerror(e)
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
	print("on_read (last:",is_last,") ",ffi.string(self.rbuf,self.ravail))
	self.avail = 0ULL
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
		local oft = 0ULL
		local sz  = ffi.sizeof(weak.self.rbuf)
		while true do
			if not weak.self then return end
			local self = weak.self
			local rd = C.read(s.socket.fd, self.rbuf + oft, sz - oft)
			-- local rd = C.read(s.socket.fd, self.rbuf + oft, 1)
			if rd >= 0 then
				-- print("read ",rd)
				self.avail = self.avail + rd;
				local avail = self.avail

				self:on_read(rd == 0)

				local pkoft = avail - self.avail
				-- print("avail ",avail, " -> ", self.avail, " pkoft = ", pkoft)


				if self.avail > 0 then
					if self.avail == self.maxbuf then
						self:on_connect_reset(box.errno.EMSGSIZE)
						return
					end
					oft = self.avail
					-- print("avail ",avail, " -> ", self.avail, " pkoft = ", pkoft)
					C.memmove(self.rbuf,self.rbuf + pkoft,oft)
				else
					if rd == 0 then
						self:on_connect_reset(box.errno.ECONNABORTED)
						return
					end
					oft = 0
				end
			elseif errno_is_transient[box.errno()] then
				self = nil
				s:readable()
			else
				-- print( box.errno.strerror( box.errno() ))
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
					box.fiber.name("C."..weak.self.port..'.'..weak.self.host)
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

function M:write_(buf)
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

function M:_wbuf_realloc( ... )
	local old = self.wbuf
	local osz = self.wsize
	self.wsize = osz * 2
	local nsz = self.wsize
	-- print("realloc from ",osz," to ",self.wsize)
	self.wbuf = ffi.new('struct iovec[?]', self.wsize)
	-- ffi.gc(self.wbuf,function ( ... )
	-- 	print("gc'ed wbuf of size ",nsz)
	-- end)
	C.memcpy(self.wbuf, old, self.wcur * ffi.sizeof(self.wbuf[0]))
end

function M:write( buf )
	self:push_write(buf)
	self:flush()
end

function M:push_write( buf, len )
	if self.wcur == self.wsize - 1 then
		self:_wbuf_realloc()
	end

	local ffibuf
	if type(buf) == 'cdata' then
		ffibuf = ffi.cast('char *',buf)
		-- ffi.gc(ffibuf,function (_)
		-- 	print("gc'ed wbuf ptr ",_)
		-- end)
	else
		ffibuf = ffi.cast('char *',buf)
		-- ffibuf = ffi.new('char[?]',len or #buf,buf)
		-- -- ffi.gc(ffibuf,function (_)
		-- -- 	print("gc'ed wbuf copy ",_)
		-- -- end)
	end

	-- print("push_write ",#buf, "; wcur = ",self.wcur, "; wstash = ", #self.wstash, "; buf = ",ffibuf)

	self.wbuf[self.wcur].iov_base = ffibuf
	self.wbuf[self.wcur].iov_len = len or #buf
	table.insert(self.wstash,buf)
	self.wcur = self.wcur + 1
end

function M:_writev()
	--- should return true if no more tries should be done


	-- local dbg
	-- for i = 0,self.wcur-1 do
	-- 	dbg = ( dbg and ( dbg .. ' + ' ) or '' ) .. tostring(i)..':'..tostring(self.wbuf[i].iov_len)..'->'..tostring(self.wbuf[i].iov_base)
	-- end
	-- print(dbg)
	-- print("call writev(",self.s.socket.fd,") with ",self.wcur, " ",self.wbuf)

	-- local wr = C.writev(self.s.socket.fd, self.wbuf, 1)
	local wr = C.writev(self.s.socket.fd, self.wbuf, self.wcur)

	-- print("written ", wr)
	if wr > 0 then
		-- if true then return end
		local len = 0
		for i = 0,self.wcur-1 do
			len = len + self.wbuf[i].iov_len
			local cptr = table.remove(self.wstash,1)
			if len == wr then
				if i == self.wcur - 1 then
					-- print("last ", len , " == ", wr)
					self.wcur = 0
					return true
				else
					-- for z = 1,i+1 do
					-- 	-- local rm =
					-- 	table.remove(self.wstash,1)
					-- 	-- print("removed from stash ",rm)
					-- end
					-- print(len , " == ", wr, " / ", i, ' ', self.wcur)
					self.wcur = self.wcur - (i+1)
					-- print("(1) new wcur = ",self.wcur, ' new wstash = ', #self.wstash)
					C.memmove( self.wbuf, self.wbuf[i+1], self.wcur * IOVSZ )
					return false
				end
			elseif len > wr then
				-- print("len ",len," > ", wr, " wcur = ", self.wcur, " i= ",i)

				local left = len - wr
				local offset = self.wbuf[i].iov_len - left

				-- for z = 0,i-1 do
				-- 	print(self.wbuf[z].iov_len, " + ")
				-- end
				-- print(offset, " of ", self.wbuf[i].iov_len, " (left ",left, " keep in stash ", self.wstash[1])

				-- why -1 ????
				-- for z = 1,i-1 do
				-- 	-- local rm =
				-- 	table.remove(self.wstash,1)
				-- 	-- print("removed from stash ",rm)
				-- end

				table.insert(self.wstash,1,cptr)

				-- local newlen = self.wbuf[i].iov_len - offset
				-- local newwbuf = ffi.new('char[?]', newlen)
				-- C.memcpy(newwbuf,ffi.cast('char *',self.wbuf[i].iov_base) + offset, newlen)

				-- print("created new buffer of size ",newlen,": ", newwbuf)

				self.wcur = self.wcur - i -- wcur - (i+1) + 1
				-- print("(2) new wcur = ",self.wcur, ' new wstash = ', #self.wstash)
				C.memmove( self.wbuf, self.wbuf[i], self.wcur * IOVSZ )

				self.wbuf[0].iov_base = ffi.cast('char *',ffi.cast('char *',self.wbuf[0].iov_base) + offset)
				self.wbuf[0].iov_len = left
				break -- for
			end
		end
	elseif errno_is_transient[ ffi.errno() ] then
		-- print(box.errno.strerror( ffi.errno() ))
		-- iowait
	else
		print(box.errno.strerror( ffi.errno() ))
		self:on_connect_reset( ffi.errno() )
		return true
	end
	-- iowait
	return false
	
end

function M:flush()
	assert(type(self) == 'table',"object required")
	
	if self.state ~= CONNECTED then
		print("flush in state ",S2S[self.state])
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
			box.raise(box.error.ER_PROC_LUA, "Not connected for flush ("..tostring(self.state)..")")
		end
	end

	if self.flushing then return end
	self.flushing = true
	box.fiber.sleep(0)

	if self:_writev() then self.flushing = false return end

	local weak = setmetatable({}, { __mode = "kv" })
	weak.self = self

	box.fiber.wrap(function(weak)
		box.fiber.name("W."..weak.self.port..'.'..weak.self.host)
		local s = weak.self.s
		local timeout = weak.self.timeout
		while weak.self do
			if s:writable() then
				if not weak.self then break end
				if weak.self:_writev() then break end
			end
		end
		if weak.self then
			weak.self.flushing = false
		end
	end,weak)		
end

return M
