#include "ipc_kv.h"

IPC_KV::IPC_KV(const std::string& name)
{
	m_name = name;

	initialize_info(m_name);
	initialize_data(m_name);
}

IPC_KV::~IPC_KV()
{
	if (m_data)
		UnmapViewOfFile(m_data);

	if (m_info)
		UnmapViewOfFile(m_info);

	if (m_data_handle)
		CloseHandle(m_data_handle);

	if (m_info_handle)
		CloseHandle(m_info_handle);
}

bool IPC_KV::set(const std::string& key, unsigned char* data, size_t size)
{
	if (size >= DATA_SIZE)
		return false;

	IPC_WriteLock write_lock(m_lock_path);

	if (LOAD_FACTOR >= MAX_LOAD_FACTOR)
		resize();
	else
		check_capacity();

	uint32_t probeIndex = 0;
	uint32_t bucketsProbed = 0;

	uint32_t hashCode = hash(key.c_str(), key.length());
	uint32_t bucket = hashCode % m_info->capacity;

	while (bucketsProbed < m_info->capacity)
	{
		if (m_data[bucket].state != IPC_KV_Data_State::Occupied || m_data[bucket].key == key)
		{ 
			if (m_data[bucket].state != IPC_KV_Data_State::Occupied)
				m_info->size++;

			m_data[bucket].size = size;
			m_data[bucket].state = IPC_KV_Data_State::Occupied;

			memcpy(m_data[bucket].data, data, size);
			strncpy_s(m_data[bucket].key, key.c_str(), key.length());

			return true;
		}

		printf("Collision! %s -> %d\n", key.c_str(), bucket);

		probeIndex++;

		bucket = (hashCode + C1_CONSTANT * probeIndex + C2_CONSTANT * probeIndex * probeIndex) % m_info->capacity;
		bucketsProbed++;
	}

	return false;
}

void IPC_KV::print()
{
	for (size_t i = 0; i < m_info->capacity; i++)
	{
		if (m_data[i].state == IPC_KV_Data_State::Occupied)
			printf("[%d] %s 0x%X\n", i, m_data[i].key, m_data[i].size);
	}

	printf("Capacity %d, Size %d, Resizes %d, Load Factor %f\n", m_info->capacity, m_info->size, m_info->resize_count, LOAD_FACTOR);
}

size_t IPC_KV::size()
{
	return m_info->size;
}

size_t IPC_KV::find_nearest_prime(size_t input)
{
	while (!is_prime(input))
	{
		input++;
	}

	return input;
}

bool IPC_KV::is_prime(size_t input) 
{
	for (int i = 2; i <= (int)sqrt(input); i++)
	{
		if (input % i == 0)
		{
			return false;
		}
	}

	return true;
}

void IPC_KV::check_capacity()
{
	if (m_cached_resize_count != m_info->resize_count)
	{
		printf("Capacity has changed, fetching new memory\n");

		if (m_data)
			UnmapViewOfFile(m_data);

		if (m_data_handle)
			CloseHandle(m_data_handle);

		initialize_data(m_name);

		m_cached_resize_count = m_info->resize_count;
		m_lock_path = std::to_string(m_info->resize_count) + "_" + m_name;
	}
}


void IPC_KV::resize()
{
	printf("Resizing memory\n");

	HANDLE previous_data_handle = m_data_handle;
	IPC_KV_Data* previous_data = m_data;
	size_t previous_capacity = m_info->capacity;

	m_info->resize_count++;
	m_info->size = 0;
	m_info->capacity = find_nearest_prime(m_info->capacity * 2);

	initialize_data(m_name);

	m_cached_resize_count = m_info->resize_count;
	m_lock_path = std::to_string(m_info->resize_count) + "_" + m_name;
	
	for (size_t i = 0; i < previous_capacity; i++)
	{
		if (previous_data[i].state == IPC_KV_Data_State::Occupied)
		{
			set(previous_data[i].key, previous_data[i].data, previous_data[i].size);
		}
	}

	if (previous_data)
		UnmapViewOfFile(previous_data);

	if (previous_data_handle)
		CloseHandle(previous_data_handle);	
}

uint32_t IPC_KV::hash(const char * key, int count)
{
	typedef uint32_t* P;
	uint32_t h = 0x811c9dc5;
	while (count >= 8) {
		h = (h ^ ((((*(P)key) << 5) | ((*(P)key) >> 27)) ^ *(P)(key + 4))) * 0xad3e7;
		count -= 8;
		key += 8;
	}
#define tmp h = (h ^ *(uint16_t*)key) * 0xad3e7; key += 2;
	if (count & 4) { tmp tmp }
	if (count & 2) { tmp }
	if (count & 1) { h = (h ^ *key) * 0xad3e7; }
#undef tmp
	return h ^ (h >> 16);
}

void IPC_KV::initialize_data(const std::string& name)
{
	auto handle_path = "ipckv_" + std::to_string(m_info->resize_count) + "_" + name;

	if (handle_path.length() > MAX_PATH)
	{
		throw std::runtime_error("key is too long.");
	}

	m_data_handle = CreateFileMapping(
		INVALID_HANDLE_VALUE,
		NULL,
		PAGE_READWRITE,
		0,
		sizeof(IPC_KV_Data) * m_info->capacity,
		handle_path.c_str()
	);

	if (m_data_handle == NULL)
	{
		throw std::runtime_error("could not create file mapping object.");
	}

	auto does_already_exist = GetLastError() == ERROR_ALREADY_EXISTS;

	auto buffer = MapViewOfFile(
		m_data_handle,
		FILE_MAP_ALL_ACCESS,
		0,
		0,
		sizeof(IPC_KV_Data) * m_info->capacity
	);

	if (buffer == NULL)
	{
		CloseHandle(m_data_handle);

		throw std::runtime_error("could not map view of file.");
	}

	m_data = (IPC_KV_Data*)buffer;

	if (!does_already_exist)
	{
		printf("Initializing data %s...\n", handle_path.c_str());
		std::memset(m_data, 0, sizeof(IPC_KV_Data) * m_info->capacity);
	}
}

void IPC_KV::initialize_info(const std::string& name)
{
	auto handle_path = "ipckv_i_" + name;

	if (handle_path.length() > MAX_PATH)
	{
		throw std::runtime_error("key is too long.");
	}

	m_info_handle = CreateFileMapping(
		INVALID_HANDLE_VALUE,
		NULL,
		PAGE_READWRITE,
		0,
		sizeof(IPC_KV_Info),
		handle_path.c_str()
	);

	if (m_info_handle == NULL)
	{
		throw std::runtime_error("could not create file mapping object.");
	}

	bool does_already_exist = GetLastError() == ERROR_ALREADY_EXISTS;

	auto buffer = MapViewOfFile(
		m_info_handle,
		FILE_MAP_ALL_ACCESS,
		0,
		0,
		sizeof(IPC_KV_Info)
	);

	if (buffer == NULL)
	{
		CloseHandle(m_info_handle);

		throw std::runtime_error("could not map view of file.");
	}

	m_info = (IPC_KV_Info*)buffer;

	if (!does_already_exist)
	{
		printf("Initializing info %s...\n", handle_path.c_str());

		m_info->size = 0;
		m_info->resize_count = 0;
		m_info->capacity = INITIAL_CAPACITY;
	}

	m_lock_path = std::to_string(m_info->resize_count) + "_" + m_name;
	m_cached_resize_count = m_info->resize_count;
}

int main()
{
	unsigned char lol[] = { 0x68, 0x69 };
	 
	auto kv = IPC_KV("test");      
	    
	kv.set("a", lol, sizeof(lol));
	kv.set("b", lol, sizeof(lol));
	kv.set("c", lol, sizeof(lol));
	kv.set("d", lol, sizeof(lol));
	kv.set("e", lol, sizeof(lol));
	kv.set("f", lol, sizeof(lol));
	kv.print();

	kv.set("Hel Worldgyuguy", lol, sizeof(lol));
	kv.print();


	printf("Finished execution, %d\n", kv.size());

	/*auto kv = new IPC_KV("test");

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

			auto object = kv->get("lol");

			printf("%s\n", object.size() ? (const char*)object.buffer() : "null");




		}
	});

	lol.detach();
	lol3.detach();
	*/
	Sleep(500000000);

	return 0;
}


/*IPC_KV::IPC_KV(std::string name) :
	m_name(std::move(name)),
	m_handle_prefix("Global\\IPCKV_" + m_name + "_"),
	m_lock_prefix("IPCKV_" + m_name + "_")
{
	std::string m_event_name = "IPCKV_" + m_name;

	if (m_event_name.length() > MAX_PATH)
	{
		throw std::runtime_error("too long name.");
	}

	m_clear_event_handle = CreateEvent(NULL, FALSE, FALSE, m_event_name.c_str());

	if (m_clear_event_handle == nullptr)
	{
		throw std::runtime_error("unable to create event for clearing. " + GetLastError());
	}

	//////////////////////////////////////////////////

	auto result = RegisterWaitForSingleObject(
		&m_clear_wait_handle, 
		m_clear_event_handle, 
		[](PVOID this_pointer, BOOLEAN b)
		{
			auto object = (IPC_KV*)this_pointer;

			object->destroy();
		},
		this, 
		INFINITE, 
		WT_EXECUTEDEFAULT
	);

	if (!result)
	{
		throw std::runtime_error("unable to register for event. " + GetLastError());
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
		IPC_KV::IPC_WriteLock lock(m_lock_prefix + item.first);
		item.second.first->size = 0;
	}
}

void IPC_KV::clear()
{
	SetEvent(m_clear_event_handle);
}

IPC_KV::IPC_Value IPC_KV::get(const std::string & key)
{
	return IPC_Value(key, m_lock_prefix + key, get_object(key));
}

IPC_KV::IPC_Object* IPC_KV::get_object(const std::string & key)
{
	std::lock_guard<std::mutex> guard(m_mutex);
	auto buffer = m_buffers.find(key);

	if (buffer == m_buffers.end())
	{
		return get_handle(key);
	}

	return buffer->second.first;
}

void IPC_KV::set(const std::string& key, const std::string& value)
{
	set(key, (const uint8_t*)value.c_str(), value.length() + 1);
}

void IPC_KV::set(const std::string& key, const uint8_t * value, const size_t len)
{
	if (len > BLOCK_SIZE)
	{
		throw std::runtime_error("object too large, increase block size");
	}

	auto ipc_object = get_object(key);

	if (ipc_object == nullptr)
	{
		auto handle_path = m_handle_prefix + key;

		if (handle_path.length() > MAX_PATH)
		{
			throw std::runtime_error("key is too long.");
		}

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
			throw std::runtime_error("could not create file mapping object. (set)");
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

			throw std::runtime_error("could not map view of file. (set)");
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
	 
	if (handle_path.length() > MAX_PATH)
	{
		throw std::runtime_error("key is too long.");
	}

	auto map_file_handle = OpenFileMapping(
		FILE_MAP_ALL_ACCESS,
		FALSE,
		handle_path.c_str()
	); 

	if (map_file_handle == NULL)
	{
		if (GetLastError() == ERROR_FILE_NOT_FOUND)
			return nullptr;

		throw std::runtime_error("could not open file mapping object. (get_handle)");
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
		throw std::runtime_error("could not map view of file. (get_handle)");
	}

	/////////////////////////////////////////////////

	auto ipc_object = (IPC_Object*)buffer;
	m_buffers[key] = std::make_pair(ipc_object, map_file_handle);

	return ipc_object;
}
*/
