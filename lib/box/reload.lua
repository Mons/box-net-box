--[[

-- usage --

box.reload:register(object) -> object:destroy()
box.reload:register(cb)     -> cb()

box.reload:register(cb,...) -> cb(...)
box.reload:register(ref,cb) -> cb(ref)

]]

local M
if not box.reload then
	M = setmetatable({
		O = setmetatable({},{
			-- __mode = 'kv'
		}),
		F = setmetatable({},{
			-- __mode = 'kv'
		}),
		C = setmetatable({},{
			-- __mode = 'kv'
		}),
	},{
		__call = function()
			print("reloading...")
			dofile("init.lua")
		end
	})
	box.reload = M
else
	M = box.reload
end

function M:cleanup()
	print("box.reload.cleanup...")
	collectgarbage()
	for ref,cb in pairs(self.O) do
		box.fiber.wrap(function() cb(ref) end)
		self.O[ref] = nil
	end
	for f in pairs(self.F) do
		box.fiber.wrap(function() f() end)
		self.F[f] = nil
	end
	for t in pairs(self.C) do
		local cb = t[1]
		table.remove(t,1)
		box.fiber.wrap(function() cb(unpack(t)) end )
		self.C[t] = nil
	end
	print("box.reload.cleanup fihished\n\n")
	collectgarbage()
end

M.deregister = M.cleanup


function M:register(...)
	assert( self == box.reload, "Static call" )
	if select('#',...) == 1 then
		local arg = ...
		print("one arg ",type(arg))
		if type(arg) == 'table' then
			if arg.destroy then
				self.O[ arg ] = arg.destroy
			else
				error("One arg call with object, but have no destroy method")
			end
		elseif type(arg) == 'function' then
			self.F[arg] = arg
		else
			error("One arg call with unsupported type: ",type(arg))
		end
	else
		local arg1 = ...
		if type(arg1) == 'function' then
			local t = {...}
			self.C[t] = t
		elseif select('#',...) == 2 then
			local ref,cb = ...
			if type(cb) == 'function' then
				self.O[ref] = cb
			else
				error("Bad arguments")
			end
		else
			error("Bad arguments: ", ...)
		end
	end
end

return M
