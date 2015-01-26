local oop = require 'box.oop'
local clr = require 'devel.caller'

require 'box.net.box'

local M = oop:define({ __name = 'box.net.pool' })
box.net.pool = M

function M:init(list, opts)
	opts = opts or {}
	-- self.class = opts.class or box.net
	self.timeout = opts.timeout or 1
	self.name = opts.name or 'pool'
	self.list = list
	
	self.c = {} -- connections
	self.f = {} -- fibers
	
	self.connected = {
		rw  = setmetatable({}, {__mode = "kv"}),
		ro  = setmetatable({}, {__mode = "kv"}),
		any = setmetatable({}, {__mode = "kv"}),
	}
	
	self.w = {
		rw  = setmetatable({}, {__mode = "kv"}),
		ro  = setmetatable({}, {__mode = "kv"}),
		any = setmetatable({}, {__mode = "kv"}),
	}
	
	local str = {}
	for _,v in ipairs(list) do
		if type(v) == 'table' then
			table.insert(str,v[1]..':'..v[2])
		else
			table.insert(str,v)
		end
	end
	print("init pool (",self.name,") with ",#list, " servers ("..table.concat(str,", ")..")")
	box.reload:register(self)
end

function M:destroy()
	for c in pairs(self.c) do
		c:destroy()
		self.c[c] = nil
	end
	for c in pairs(self.f) do
		pcall(box.fiber.cancel,self.f[c] )
		self.f[c] = nil
	end
	local name = self.name
	for k in pairs(self) do
		self[k] = nil
	end
	setmetatable(self,{
		__index = function(s,name)
			print("access to `"..name.."' on destroyed pool "..name..clr(1))
			box.fiber.cancel(box.fiber.self())
		end,
		__newindex = function(s,name)
			print("access to `"..name.."' on destroyed pool "..name..clr(1))
			box.fiber.cancel(box.fiber.self())
		end
	})
	--[[
	self.connected = {
		rw  = setmetatable({}, {__mode = "kv"}),
		ro  = setmetatable({}, {__mode = "kv"}),
		any = setmetatable({}, {__mode = "kv"}),
	}
	self.w = {
		rw  = setmetatable({}, {__mode = "kv"}),
		ro  = setmetatable({}, {__mode = "kv"}),
		any = setmetatable({}, {__mode = "kv"}),
	}
	]]
end

function M:connect()
	for _,v in pairs(self.list) do
		local host,port,alias
		if type(v) == 'table' then
			host,port,alias = unpack(v)
		else
			host,port = v:match('^([^:]+):([^:]+)$')
		end
		if not alias then
			if host:match('^%d+%.%d+%.%d+%.%d+$') then
				alias = host
			else
				alias = host:match('^[^.]+')
			end
		end
		
		local gen = 0
		local con = box.net.box(host, port, { timeout = self.timeout, deadly = false })
		
		self.c[con] = con
		
			con.on_connected = function(s)
				self.f[ s ] = box.fiber.self()
				box.fiber.name(self.name..'#'..tostring(gen)..':'..alias..':'..port)
				gen = gen + 1
				local cg = gen
				local isrw
				local first = true
				while cg == gen do
					local r,e = pcall(function()
						return con:call('box.dostring','return box.info.status')
					end)
					local rw
					if r then
						if e[0] == 'primary' then rw = true
						elseif string.find(e[0],'replica') then rw = false else
							print( "Unknown status from db: ",e )
						end
						--print(e[0], " ", rw, " ",isrw)
						if first then
							s:log("I","connected for mode %s (%s)",rw and "rw" or "ro", e[0])
							if rw then
								table.insert(self.connected.rw, con)
								for c in pairs(self.w.rw) do
									c:put(true)
								end
							elseif rw ~= nil then
								table.insert(self.connected.ro, con)
								for c in pairs(self.w.ro) do
									c:put(true)
								end
							end
								table.insert(self.connected.any, con)
								for c in pairs(self.w.any) do
									c:put(true)
								end
							
							isrw = rw
							first = false
						elseif rw ~= isrw then
							s:log("I","switch mode %s->%s (%s)",isrw and "rw" or "ro", rw and "rw" or "ro", e[0])
							for _,t in pairs({"rw","ro"}) do
								for k,v in pairs(self.connected[t]) do
									if con == v then
										table.remove(self.connected[t],k)
										break
									end
								end
							end
							if rw then
								table.insert(self.connected.rw, con)
							elseif rw ~= nil then
								table.insert(self.connected.ro, con)
							end
							isrw = rw
						end
					else
						s:log('W',"status request failed: %s (%s/%s)", e, cg,gen)
						-- return
					end
					box.fiber.sleep( self.timeout/3 )
				end
				self.f[ s ] = nil
			end
			con.on_disconnect = function(s,e)
				gen = gen + 1
				s:log('W',"connection reset: %s", e)
				for st,c in pairs(self.connected) do
					for k,v in pairs(c) do
						if con == v then
							table.remove(c,k)
							break
						end
					end
				end
			end
			con:connect()
	end
	return self
end

function M:wait(key, to)
	key = key or "any"
	if not ( key == "rw" or key == "ro" or key == "any" ) then
		error("Key must be rw/ro/any")
	end
	local ch = box.ipc.channel()
	self.w[ key ][ ch ] = ch
	local r
	print("wait for ",key)
	if to then
		r = ch:get(to)
	else
		r = ch:get()
	end
	return r
end

function M:ready()
	return #self.connected.any > 0
end

function M:any()
	if #self.connected.any > 0 then
		return self.connected.any[ math.random(1, #self.connected.any) ]
	else
		return
	end
end

function M:rw()
	if #self.connected.rw > 0 then
		return self.connected.rw[ math.random(1, #self.connected.rw) ]
	else
		return
	end
end

function M:ro()
	if #self.connected.ro > 0 then
		return self.connected.ro[ math.random(1, #self.connected.ro) ]
	else
		return
	end
end

return M
