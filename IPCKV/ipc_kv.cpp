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
	IPC_ReadLock read_lock(m_lock_path);

	check_capacity();

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
	printf("Resizing memory.\n");

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

	while (1)
	{
		std::string key;

		getline(std::cin, key);

		if (!key.empty())
		{
			printf("Inserting %s\n", key.c_str());
			kv.set(key, lol, sizeof(lol));
		}

		kv.print();
	}

	printf("Finished execution, %d\n", kv.size());

	
	Sleep(500000000);

	return 0;
}