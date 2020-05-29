#include "ipc_kv.h"

IPC_KV::~IPC_KV()
{
	for (auto buffer : m_buffers)
	{
		UnmapViewOfFile(buffer.second);
	}

	for (auto handle : m_handles)
	{
		CloseHandle(handle.second);
	}
}

IPC_KV::IPC_Object* IPC_KV::get(const std::string & key)
{
	auto buffer = m_buffers.find(key);

	if (buffer == m_buffers.end())
	{
		return get_handle(key);
	}

	return buffer->second;
}

bool IPC_KV::set(const std::string& key, const uint8_t * value, const size_t len)
{
	if (len > BLOCK_SIZE)
	{
		printf("Object too large.");
		return false;
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
			printf("Could not create file mapping object: 0x%X\n", GetLastError());

			return false;
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
			printf("Could not map view of file: 0x%X\n", GetLastError());

			CloseHandle(map_file_handle);

			return false;
		}

		ipc_object = (IPC_Object*)buffer;
	}

	//////////////////////////////////////////
	
	auto mutex_name = m_mutex_prefix + key;

	auto mutex_handle = CreateMutex(
		NULL,
		FALSE,
		mutex_name.c_str()
	);

	if (mutex_handle == NULL)
	{
		printf("Could not create mutex: 0x%X\n", GetLastError());
		return false;
	}

	//////////////////////////////////////////

	memcpy(ipc_object->buffer, value, len);
	ipc_object->size = len;	

	CloseHandle(mutex_handle);

	return true;
}

IPC_KV::IPC_Object* IPC_KV::get_handle(const std::string & key)
{
	std::string handle_path = m_handle_prefix + key;

	/////////////////////////////////////////////////
	 
	auto map_file_handle = OpenFileMapping(
		FILE_MAP_ALL_ACCESS,
		FALSE,
		handle_path.c_str()
	); 

	if (map_file_handle == NULL)
	{
		printf("Could not open file mapping object: 0x%X\n", GetLastError());

		return nullptr;
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
		printf("Could not map view of file: 0x%X\n", GetLastError());

		return nullptr;
	}

	/////////////////////////////////////////////////

	auto ipc_object = (IPC_Object*)buffer;

	m_handles[key] = map_file_handle;
	m_buffers[key] = ipc_object;

	return ipc_object;
}


int main()
{
	IPC_KV kv("test");
	IPC_KV kv2("test2");

	const uint8_t lol[] = {
		0x68, 0x69, 0x00
	}; 

	const uint8_t lol2[] = {
		0x68, 0x69, 0x68, 0x00
	};

	
	kv.set("lol", lol, sizeof lol);
	kv.set("lol", lol, sizeof lol);
	kv2.set("lol", lol2, sizeof lol2);

	auto object = kv.get("lol");

	if (object)
		printf("%s\n", object->buffer);
	else
		printf("null\n");




	system("pause");

	return 0;
}