#include "ipc_kv.h"

IPC_KV::IPC_KV(std::string name) :
	m_name(std::move(name)),
	m_handle_prefix("Global\\IPCKV_" + m_name + "_"),
	m_lock_prefix("IPCKV_" + m_name + "_")
{
	std::string m_event_name = "IPCKV_" + m_name;

	if (m_event_name.length() > MAX_PATH)
	{
		throw std::exception("too long name.");
	}

	m_clear_event_handle = CreateEvent(NULL, FALSE, FALSE, m_event_name.c_str());

	if (m_clear_event_handle == nullptr)
	{
		throw std::exception("unable to create event for clearing. " + GetLastError());
	}

	//////////////////////////////////////////////////

	auto clear_callback = [](PVOID p, BOOLEAN b) 
	{
		auto object = (IPC_KV*)p;

		object->destroy();
	};

	auto result = RegisterWaitForSingleObject(
		&m_clear_wait_handle, 
		m_clear_event_handle, 
		clear_callback, 
		this, 
		INFINITE, 
		WT_EXECUTEDEFAULT
	);

	if (!result)
	{
		throw std::exception("unable to register for event. " + GetLastError());
	}
}

IPC_KV::~IPC_KV()
{
	UnregisterWait(m_clear_wait_handle);
	CloseHandle(m_clear_event_handle);

	std::lock_guard<std::mutex> guard(m_mutex);
	for (auto it = m_buffers.cbegin(); it != m_buffers.cend();)
	{
		UnmapViewOfFile(it->second.first);
		CloseHandle(it->second.second);

		it = m_buffers.erase(it);
	}
}

void IPC_KV::destroy()
{
	std::lock_guard<std::mutex> guard(m_mutex);
	for (auto item : m_buffers)
	{
		if (item.second.first->size)
		{
			IPC_KV::IPC_WriteLock lock(m_lock_prefix + item.first);
			item.second.first->size = 0;
		}
	}
}

void IPC_KV::clear()
{
	SetEvent(m_clear_event_handle);
}

IPC_KV::IPC_Value IPC_KV::get(const std::string & key)
{
	std::lock_guard<std::mutex> guard(m_mutex);
	auto buffer = m_buffers.find(key);

	if (buffer == m_buffers.end())
	{
		return IPC_Value(key, get_handle(key));
	}

	return IPC_Value(key, buffer->second.first);
}

void IPC_KV::set(const std::string& key, const std::string& value)
{
	set(key, (const uint8_t*)value.c_str(), value.length() + 1);
}

void IPC_KV::set(const std::string& key, const uint8_t * value, const size_t len)
{
	if (len > BLOCK_SIZE)
	{
		throw std::exception("object too large, increase block size");
	}

	auto ipc_object = get(key);

	if (ipc_object == nullptr)
	{
		auto handle_path = m_handle_prefix + key;

		auto map_file_handle = CreateFileMapping(
			INVALID_HANDLE_VALUE, 
			NULL,                   
			PAGE_READWRITE,          
			0,                      
			sizeof(IPC_Object),
			handle_path.c_str()
		);  

		if (map_file_handle == NULL)
		{
			throw std::exception("could not create file mapping object (set)");
		}

		auto buffer = MapViewOfFile(
			map_file_handle,
			FILE_MAP_ALL_ACCESS,
			0,
			0,
			sizeof(IPC_Object)
		);

		if (buffer == NULL)
		{
			CloseHandle(map_file_handle);

			throw std::exception("could not map view of file. (set)");
		}

		////////////////////////////////////////////////

		ipc_object = (IPC_Object*)buffer;

		std::lock_guard<std::mutex> guard(m_mutex);
		m_buffers[key] = std::make_pair(ipc_object, map_file_handle);
	}

	IPC_KV::IPC_WriteLock lock(m_lock_prefix + key);

	memcpy(ipc_object->buffer, value, len);
	ipc_object->size = len;
}

IPC_KV::IPC_Object* IPC_KV::get_handle(const std::string & key)
{
	auto handle_path = m_handle_prefix + key;
	 
	auto map_file_handle = OpenFileMapping(
		FILE_MAP_ALL_ACCESS,
		FALSE,
		handle_path.c_str()
	); 

	if (map_file_handle == NULL)
	{
		if (GetLastError() == ERROR_FILE_NOT_FOUND)
			return nullptr;

		throw std::exception("could not open file mapping object. (get_handle)");
	}

	/////////////////////////////////////////////////

	auto buffer = MapViewOfFile(
		map_file_handle,
		FILE_MAP_ALL_ACCESS,
		0,
		0,
		sizeof(IPC_Object)
	);

	if (buffer == NULL)
	{
		throw std::exception("could not map view of file. (get_handle)");
	}

	/////////////////////////////////////////////////

	auto ipc_object = (IPC_Object*)buffer;
	m_buffers[key] = std::make_pair(ipc_object, map_file_handle);

	return ipc_object;
}

int main()
{
	auto kv = new IPC_KV("test");

	std::thread lol([kv]() {
		while (true) 
		{
			kv->set("lol", "hello world2");
		} 
	});
	
	std::thread lol3([kv]() {
		while (true)
		{
			if (GetAsyncKeyState(VK_DELETE))
				kv->clear();

			printf("%s\n", kv->get("lol") && kv->get("lol")->size ? (const char*)kv->get("lol")->buffer : "null");
		}
	}); 

	lol.detach();
	lol3.detach();
	
	Sleep(500000000);

	return 0;
}