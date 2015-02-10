dofile 'box/steady.lua'

require 'strict'
require 'box.net.pool'

--local oop = require 'box.oop'

--[[
local f = box.fiber.wrap(function()
	while true do
		print("alive")
		box.fiber.sleep(1/3)
	end
end)

print(f, " ", f.__gc)
f = nil
if true then return end
]]

--[[
-- test 1
local con = box.net( 'cld-test.dev.mail.ru',3015 )
local weak = setmetatable({}, { __mode = "kv" })
weak.obj = con
con:connect()
con = nil
collectgarbage()
print(weak.obj)
]]

--[[
-- test 2

local con = box.net( 'cld-test.dev.mail.ru',3015 )
local weak = setmetatable({}, { __mode = "kv" })
weak.obj = con
con:connect()


box.fiber.wrap(function()
	box.fiber.sleep(1/3)
	con = nil
	collectgarbage()
	if weak.obj then
		
	else
		
	end
	print("still have object: ",weak.obj)
end)
]]



--[[


con.on_connected = function(self)
	print("connected ",self, " ",self.state)
	self:write("lua box.fiber.sleep(0.1) print('test')\n")
	-- local buf = "GET / HTTP/1.0\n\n"
	-- self:write(buf)
end
con.on_read = function(self,last)
	print(self.rbuf, last)
	self.rbuf = ''
	self:write("lua box.fiber.sleep(0.5) print('test')\n")
end

con:connect()

box.fiber.wrap(function()
	box.fiber.sleep(1.5)
	print("destroy ")
	con = nil
end)
]]


local pool = box.net.pool({
	-- {'cld-test.dev.mail.ru', 3013},
	-- {'cld-test.dev.mail.ru', 3113},
	 { '127.10.1.0', 33013 },
	-- { '127.0.0.1', 33013 },
	-- { '127.0.0.1', 3313 }, -- self
}, { timeout = 1 })

pool:connect()

box.fiber.wrap(function()
	box.fiber.name("waiter");
	print("wait for online")
	pool:wait()
	print("online")
	local seq = 0
	while true do
		local c = pool:any()
		if c then
			seq = seq + 1
			local n = seq
			print("call for ",n)
			local r,e = pcall(function()
				local r = c:call('box.dostring', 'box.fiber.sleep(0.9) return "_______ ok:'..tostring(n)..'"')
				return r
			end)
			print(n,' : ',e)
		else
			pool:wait()
		end
	end
end)

--dofile("t.lua")
--dofile("t1.lua")

print("init done")
