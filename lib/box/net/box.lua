require "box.net"
local object = require 'box.oop'


--local string = require "string"
--local debug  = require "debug"

--local base = _G

-- module("box.net.box")

--local print    = base.print
--local tonumber = base.tonumber
--local tostring = base.tostring
--local error    = base.error

box.net.self = {
	__name = 'box.net.self',
	
	process = function(self, ...)
		return box.process(...)
	end,
	
	request = function(self,...)
		return box.process(...)
	end,
	
	select_range = function(self, sno, ino, limit, ...)
		return box.space[tonumber(sno)].index[tonumber(ino)]
			:select_range(tonumber(limit), ...)
	end,
	
	select_reverse_range = function(self, sno, ino, limit, ...)
		return box.space[tonumber(sno)].index[tonumber(ino)]
			:select_reverse_range(tonumber(limit), ...)
	end,
	
	call = function(self, proc_name, ...)
		--[[
			return                        ()
			return 123                    tuple({123})
			return {123}                  tuple({123})
			return {123,456}              tuple({123,456})
			
			return {{123}}                (tuple({123}))                   reduced to {123}
			
			return 123,456                tuple({123}),tuple({456})
			return {{123},{456}}          (tuple({123}), tuple({456}))     reduced to {123},{456}
		]]--
		local proc = box.call_loadproc(proc_name)
		local rv = { proc(...) }
		if #rv == 0 then return end
		if type(rv[1]) == 'table' and #rv[1] > 0 and type(rv[1][1]) == 'table' then
			rv = rv[1]
		end
		for k,v in pairs(rv) do
			if type(v) ~= 'userdata' then
				rv[k] = box.tuple.new( v )
			end
		end
		return unpack(rv)
	end,
	
	ecall = function(self, proc_name, ...)
		local proc = box.call_loadproc(proc_name)
		local rv = { proc(...) }
		
		if #rv == 0 then return end
		if type(rv[1]) == 'table' and #rv[1] > 0 and type(rv[1][1]) == 'table' then
			rv = rv[1]
		end
		if type(rv[1]) ~= 'userdata' then
			rv[1] = box.tuple.new( rv[1] ):totable()
		end
		return unpack(rv[1])
	end,
	
	ping = function(self)
		return true
	end,
	
	timeout = function(self, timeout)
		return self
	end,
	
	close = function(self)
		return true
	end
}

box.net.box = box.net:inherits({
	__name = 'box.net.box'
})

setmetatable(box.net.self, {
	__index = box.net.box,
	__tostring = function(self) return 'box.net.self()' end,
})

local seq = 0
function box.net.box.seq()
	seq = seq + 1
	if seq < 0xffffffff then
	else
		seq = 1
	end
	return seq
end

function box.net.box:on_connected()
	self.req = {}
end

function box.net.box:reader()
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

function box.net.box:request(pktt,body)
	if self.state == 'auto' then
		self.state = 'init'
		local connected = self:connect(false)
		if not connected then return false end
	end
	if self.state ~= 'connected' then return false end
	local sync = self:seq()
	local req  = box.pack('iiia', pktt, string.len(body), sync, body)
	--- printf("state: %s: sending %d bytes...", self.state, #req)
	self.req[sync] = box.ipc.channel(1)
	self.wchan:put(req)
	-- todo: timeout
	local res = self.req[sync]:get()
	---- print("received ",res)
	if res then
		local op,body = unpack(res)
		if op ~= pktt then error("Packet type mismatch") end
		if op == 65280 then return true end
		local code,resp = box.unpack('ia',body)
		if code ~= 0 then box.raise(code, resp) end
		return box.unpack('R',resp)
	else
		error("Not received res")
	end
end

function box.net.box:ping()
	-- if self.state ~= 'connected' and self.state ~= 'auto' then return false end
	return self:request(65280,'')
end

function box.net.box:call(proc, ...)
	local cnt = select('#',...)
	-- local r = 
	return self:request(22,
		box.pack('iwaV',
			0,                      -- flags
			#proc,
			proc,
			cnt,
			...
		)
	)
	--if r then
	--	if #r >= 1 then return unpack(r) end
	--end
	--if #r == 1 then return r[1]:unpack() end
	-- return
end

function box.net.box:ecall(proc,...)
		--[[
			return                        ()                               must be ()
			return {{}}                   tuple()                          would be ''
			return 123                    tuple({123})                     must be (123)
			return {123}                  tuple({123})                     must be (123)
			return {123,456}              tuple({123,456})                 must be (123,456)
			
			return {{123}}                tuple({123})                     must be (123)
			
			return 123,456                tuple({123}),tuple({456})        such return value prohibited. would be (123)
			return {{123},{456}}          (tuple({123}), tuple({456}))     such return value prohibited. would be (123)
		]]--
	local cnt = select('#',...)
	local r = 
	{ self:request(22,
		box.pack('iwaV',
			0,                      -- flags
			#proc,
			proc,
			cnt,
			...
		)
	) }
	if #r == 0 then return end
	local t = r[1]:totable()
	return (unpack( t ))
end

function box.net.box:delete(space,...)
	local key_part_count = select('#', ...)
	local r = self:request(21,
		box.pack('iiV',
			space,
			box.flags.BOX_RETURN_TUPLE,  -- flags
			key_part_count, ...
		)
	)
	return r
end

function box.net.box:replace(space, ...)
	local field_count = select('#', ...)
	return self:request(13,
		box.pack('iiV',
			space,
			box.flags.BOX_RETURN_TUPLE,
			field_count, ...
		)
	)
end

function box.net.box:insert(space, ...)
	local field_count = select('#', ...)
	return self:request(13,
		box.pack('iiV',
			space,
			bit.bor(box.flags.BOX_RETURN_TUPLE,box.flags.BOX_ADD),
			field_count, ...
		)
	)
end

function box.net.box:update(space, key, format, ...)
	local op_count = select('#', ...)/2
	return self:request(19,
		box.pack('iiVi'..format,
			space,
			box.flags.BOX_RETURN_TUPLE,
			1, key,
			op_count, ...
		)
	)
end

function box.net.box:select( space, index, ...)
	return self:select_limit(space, index, 0, 0xffffffff, ...)
end

function box.net.box:select_limit(space, index, offset, limit, ...)
	local key_part_count = select('#', ...)
	return self:request(17,
		box.pack('iiiiiV',
			space,
			index,
			offset,
			limit,
			1, -- key count
			key_part_count, ...
		)
	)
end

function box.net.box:select_range( sno, ino, limit, ...)
	return self:call(
		'box.select_range',
		tostring(sno),
		tostring(ino),
		tostring(limit),
		...
	)
end

function box.net.box:select_reverse_range( sno, ino, limit, ...)
	return self:call(
		'box.select_reverse_range',
		tostring(sno),
		tostring(ino),
		tostring(limit),
		...
	)
end

return box.net.box
