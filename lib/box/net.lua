-- local string = require "string"
-- local debug  = require "debug"

local object = require "box.oop"
local log = require "box.log"

-- local ipc    = box.ipc
-- local fiber  = box.fiber

-- local base = _G
-- log.modlevel(log.DEBUG)
-- module("box.net", package.seeall)

---- test
-- local print    = base.print
-- local tonumber = base.tonumber
-- local tostring = base.tostring
-- local error    = base.error

box.net = object:define({ __name = 'box.net' })

-- base.box.net = box.net

-- print("net = ",box.net)

function box.net:init( host, port, timeout )
	self.host = host;
	self.port = tonumber(port);
	self._timeout = timeout or 1.0;
	--self.state = 'init'
	self.state = 'auto'
	-- states: init, connecting, connected, reconnecting [, resolve ]
	log.debug("boxinit: %s:%s/%f",self.host,self.port,self._timeout)
	box.reloader:register(self)
end

function box.net:destroy()
	log.info("destroy box.net.box: ", self)
	box.net.close(self)
	if self.wrfib then
		if box.fiber.status(self.wrfib) ~= 'dead' then
			log.info("Cancel wr:",box.fiber.id(self.wrfib))
			box.fiber.cancel(self.wrfib)
		end
	end
	if self.rw then
		if box.fiber.status(self.rw) ~= 'dead' then
			local fib = box.fiber.id(self.rw)
			box.info("Cancel rw:",fib)
			box.fiber.cancel(self.rw)
		end
	end
	self = nil
	collectgarbage()
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

function box.net:fatal(message,...)
	-- print(string.format(message, ...))
	self.s:close()
	self.s = nil
	self.wchan:close()
end

function box.net:close()
	-- TODO: correct close in each state
	if self.s then
		self.s:close()
		self.s = nil
	end
	if self.wchan then
		self.wchan:close()
	end
end

function box.net:reader()
	error("Not implemented")
end

function box.net.writer(self)
	box.fiber.name("writer."..tostring(self))
	while self.s do
		-- print("wait for write data")
		local wbuf = self.wchan:get()
		if wbuf then
			-- printf("got %d bytes to write",#wbuf)
			local res = { self.s:send(wbuf) }
			if res[1] ~= string.len(wbuf) then
				self:fatal("Error while write socket: %s", res[4])
			end
		else
			log.error("no wbuf from wchan")
		end
	end
	return
end

function box.net.connreader(self)
	box.fiber.name("rw-cn." .. self:desc())
	while true do
		while true do
			--- printf("connecting to %s:%s", self.host,self.port)
			self.state = 'connecting'
			local begin = box.time()
			self.s = box.socket.tcp() or error("failed to create socket")
			local status = { self.s:connect( self.host, self.port, self._timeout ) }
			if status[1] == nil then
				--- printf("connect to %s:%s failed: %s", self.host, self.port, status[4])
				self.state = 'reconnecting'
				if not self.connwait then
					if self.cchan then
						self.cchan:put(false)
						---- print("cchan sent")
						self.cchan = nil
					end
				end
				box.fiber.sleep(self._timeout - ( box.time() - begin ))
				self.s = nil
			else
				box.fiber.name("rw-cn." .. self:desc())
				self.state  = 'connected'
				break;
			end
		end
		
		self:on_connected()
		
		--- print("connected")
		if self.cchan then
			self.cchan:put(true)
			-- print("cchan sent")
			self.cchan = nil
		end
		
		self.wchan  = box.ipc.channel(1)
		self.wrfib  = box.fiber.wrap(self.writer,self)
		box.fiber.testcancel()
		self:reader()
		box.fiber.testcancel()
		
		print("reader left")
	end
end

-- connect() | connect(nil) - no wait
-- connect(false) - wait once
-- connect(true) - wait until connected

function box.net:connect(waitready)
	if self.state ~= 'init' then return end
	log.debug("called connect in state %s / %s, wait = %s",self.state, self.s, waitready)
	self.connwait = waitready
	
	if self.connwait ~= nil then
		self.cchan = box.ipc.channel(1)
	end
	self.rw = box.fiber.wrap(self.connreader,self)
	if self.connwait == nil then
		return nil
	end
	--- printf("waiting on cchan %s %s",self.cchan, self.connwait and "forever" or "once")
	local x = self.cchan:get()
	--- print("got cchan ",x)
	return x
end

function box.net:on_connected() end


function box.net:timeout(t)
	local wrapper = {}
	setmetatable(wrapper,{
		__index = function(wself, name, ...)
			local func = self[name]
			if func then
				return
				function(_, ...)
					self._local_timeout = t
					return func(self, ...)
				end
			end
			error("Can't find " .. self.__name .. ":" .. name .. " method")
		end
	})
	return wrapper
end

return box.net

--[[


function box.con.box:reader()
	while self.s do
		local res = { self.s:recv(12) }
		if res[4] ~= nil then
			self:fatal("Can't read from %s: %s", self.s, res[3])
			return
		end
		local header = res[1]
		if string.len(header) ~= 12 then
			self:fatal("Short read from %s: %s", self.s, res[3] or res[2])
			return
		end
		local op, blen, sync = box.unpack('iii', header)
		--- print("got header ", op,' ', blen, ' ', sync)
		local body
		if blen > 0 then
			res = { self.s:recv(blen) }
			if res[4] ~= nil then
				self:fatal("Can't read from %s: %s", self.s, res[3])
				return
			end
			body = res[1]
			if string.len(body) ~= blen then
				self:fatal("Short read from %s: %s", self.s, res[3] or res[2])
				return
			end
		end
		if self.req[sync] ~= nil then
			self.req[sync]:put({ op,body })
		else
			printf("Unexpected packet %s:%s from %s",op,sync,self.s)
		end
	end
end

function box.con.box:ping()
	-- if self.state ~= 'connected' and self.state ~= 'auto' then return false end
	return self:request(65280,'')
end

function box.con.box:call(proc, ...)
	local cnt = select('#',...)
	local r = self:request(22,
		box.pack('iwaV',
			0,                      -- flags
			#proc,
			proc,
			cnt,
			...
		)
	)
	if r then
		if #r >= 1 then return unpack(r) end
	end
	--if #r == 1 then return r[1]:unpack() end
	return
end

function box.con.box:request(pktt,body)
	if self.state == 'auto' then
		print("autoconnect")
		self.state = 'init'
		local connected = self:connect(false)
		if not connected then return false end
	end
	if self.state ~= 'connected' then return false end
	local sync = box.con.box.seq()
	local req  = box.pack('iiia', pktt, string.len(body), sync, body)
	--- printf("state: %s: sending %d bytes...", self.state, #req)
	self.req[sync] = box.ipc.channel(1)
	self.wchan:put(req)
	local res = self.req[sync]:get()
	---- print("received ",res)
	if res then
		local op,body = unpack(res)
		if op ~= pktt then error("Packet type mismatch") end
		if op == 65280 then return true end
		local code,resp = box.unpack('ia',body)
		if code ~= 0 then box.raise(code, resp) end
		return { box.unpack('R',resp) }
	else
		error("Not received res")
	end
end


--local cnn = box.con("127.0.0.1",33013)
box.fiber.wrap(function()
	box.fiber.name("main")
	--box.fiber.detach()
	local cnn = box.con.box("127.1.0.1",33013)
	print("new object = ",cnn)
	-- cnn:connect()
	-- print("connected:",cnn)
	while true do
		--cnn:send("GET / HTTP/1.0\n\n")
		print("ping: ",cnn:ping())
		box.fiber.sleep(1)
		print("call:",cnn:call('testsleep',"0.01"))
		box.fiber.sleep(1)
	end
end)



]]--
