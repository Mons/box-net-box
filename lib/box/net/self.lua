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

setmetatable(box.net.self, {
	__index = box.net.box,
	--__tostring = function(self) return 'box.net.self()' end,
})
