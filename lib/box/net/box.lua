local net = require 'box.net'

local M = net:inherits({ __name = 'box.net.box' })
box.net.box = M

require 'box.net.self'

local C2R = {
	[65280] = 'ping',
	[13]    = 'insert',
	[17]    = 'select',
	[19]    = 'update',
	[21]    = 'delete',
	[22]    = 'call',
}

local seq = 0
function M.seq()
	seq = seq < 0xffffffff and seq + 1 or 1
	return seq
end

function M:init(...)
	getmetatable( self.__index ).__index.init( self,... )
	self.req = setmetatable({},{__mode = "kv"})
end

function M:_cleanup(e)
	getmetatable( self.__index ).__index._cleanup( self,e )
	for k,v in pairs(self.req) do
		if type(v) ~= 'number' then
			v:put(false)
		end
		self.req[k] = nil
	end
end

function M:on_read(is_last)
	-- print("<<", self.rbuf:xd())
	local oft = 0
	while #self.rbuf >= oft + 12 do
		local pktt,len,seq = box.unpack('iii',string.sub(self.rbuf, oft+1,oft+12 ))
		--print("packet ",pktt," ",len," ",seq)
		if #self.rbuf >= oft + 12 + len then
			local body = string.sub( self.rbuf,oft + 12 + 1,oft + 12 + len )
			--print("body = '",body:xd(),"'")
			
			if self.req[ seq ] then
				if type(self.req[ seq ]) ~= 'number' then
					--print("Have requestor for ",seq)
					self.req[ seq ]:put( body )
					self.req[ seq ] = nil
				else
					print("Received timed out ",C2R[ pktt ], "#",seq," after ",string.format( "%0.4fs", box.time() - self.req[ seq ] ))
					self.req[ seq ] = nil
				end
			else
				print("Got no requestor for ",seq)
			end
			-- ...
			
			oft = oft + 12 + len
		else
			break
		end
	end
	if oft > 0 then
		if oft < #self.rbuf then
			self.rbuf = string.sub( self.rbuf,oft+1 )
		else
			self.rbuf = ''
		end
	end
	return
end

function M:request(pktt,body)
	local seq = self.seq()
	local req = box.pack('iiia', pktt, #body, seq, body)
	-- print("request ",C2R[pktt] or pktt, "#", seq)
	self:write(req)
	local ch = box.ipc.channel(1)
	self.req[ seq ] = ch
	local now = box.time()
	local body = ch:get( self.timeout ) -- timeout?
	--print("got body = ",body, " ",self.lasterror)
	if body then
		if #body > 0 then
			local code,resp = box.unpack('ia',body)
			if code ~= 0 then
				box.raise(code, resp)
			end
			return box.unpack('R',resp)
		else
			return ''
		end
	elseif body == false then
		self.req[ seq ] = nil
		print("Request ",C2R[ pktt ], "#",seq," error: "..self.lasterror.." after ",string.format( "%0.4fs", box.time() - now ))
		box.raise( box.error.ER_PROC_LUA, "Request "..C2R[ pktt ].."#"..seq.." error: "..self.lasterror )
	else
		self.req[ seq ] = now
		print("Request ",C2R[ pktt ], "#",seq," timed out after ",string.format( "%0.4fs", box.time() - now ))
		box.raise( box.error.ER_PROC_LUA, "Request "..C2R[ pktt ].."#"..seq.." timed out" )
	end
end

function M:ping()
	local res,err = pcall(self.request, self, 65280,'')
	return res
end

function M:call(proc,...)
	local cnt = select('#',...)
	return self:request(22,
		box.pack('iwaV',
			0, -- flags
			#proc,
			proc,
			cnt,
			...
		)
	)
end

function M:lua(proc,...)
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
M.ecall = M.lua

function M:delete(space,...)
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

function M:replace(space, ...)
	local field_count = select('#', ...)
	return self:request(13,
		box.pack('iiV',
			space,
			box.flags.BOX_RETURN_TUPLE,
			field_count, ...
		)
	)
end

function M:insert(space, ...)
	local field_count = select('#', ...)
	return self:request(13,
		box.pack('iiV',
			space,
			bit.bor(box.flags.BOX_RETURN_TUPLE,box.flags.BOX_ADD),
			field_count, ...
		)
	)
end

function M:update(space, key, format, ...)
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

function M:select( space, index, ...)
	return self:select_limit(space, index, 0, 0xffffffff, ...)
end

function M:select_limit(space, index, offset, limit, ...)
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

function M:select_range( sno, ino, limit, ...)
	return self:call(
		'box.select_range',
		tostring(sno),
		tostring(ino),
		tostring(limit),
		...
	)
end

function M:select_reverse_range( sno, ino, limit, ...)
	return self:call(
		'box.select_reverse_range',
		tostring(sno),
		tostring(ino),
		tostring(limit),
		...
	)
end
