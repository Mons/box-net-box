-- for good lua reloading

if not bkp then rawset(_G,'bkp',{_first = true, count = 0}) end
if bkp._first then
	bkp.package = package
	bkp.require = require
	bkp.first   = true
	bkp._first  = false
	
	print("Loading lua 1st time")
	require 'box.reload'
	
	-- useful extensions
	rawset(_G,'NULL',box.cjson.decode( 'null'))
	rawset(_G,'FIRST',true)
	
	function string.tohex(str)
		local r = str:gsub('.', function (c) return string.format('%02X', string.byte(c)) end)
		return '\\x'..r
	end
	
	function string.xd(str)
		local r = str:gsub('.', function (c) return string.format('%02X ', string.byte(c)) end)
		return r
	end
	
	if box.time64 == nil then
		box.time64 = function()
			return tonumber64(os.time()*(1e6))
		end
	end
	--[[
	-- deadly fibers
	do
		local ffi = require 'ffi'
		
		ffi.cdef ' typedef struct { long fid; } fiber_guard_t; '
		local guard = ffi.metatype('fiber_guard_t', {
			__gc = function( x )
				local fid = tonumber(x.fid)
				local f = box.fiber.find( fid )
				if f then
					box.fiber.cancel(f)
				end
			end
		})
		
		function fiber(fn, name, ...)
			local f = box.fiber.create(function (...)
				if name then box.fiber.name(name) end
				box.fiber.detach()
				fn(...)
			end)
			box.fiber.resume(f,...)
			return setmetatable({
				__guard = guard({ box.fiber.id(f) })
			},{
				__index = f
			})
		end
	end
	]]
else
	bkp.first = false
	bkp.count = bkp.count + 1
	package   = bkp.package
	require   = bkp.require
	rawset(_G,'FIRST',false)
	print("Reloading lua (",bkp.count,")")
	box.reload:cleanup()
end
