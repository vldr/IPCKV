#include "ipc_kv.h"

bool should_crash = false;

IPC_KV::IPC_KV(const std::string& name)
{
	m_name = name;
	m_controller = new IPC_KV_Controller();

	initialize_info(m_name);
	initialize_data(m_name); 
}

IPC_KV::~IPC_KV()
{
	if (m_controller)
		delete m_controller;
}

void IPC_KV::initialize_info(const std::string& name)
{
	auto handle_path = "ipckv_i_" + name;

	if (handle_path.length() > MAX_PATH)
	{
		throw std::runtime_error("key is too long.");
	}
	
	//////////////////////////////////////////////////

	auto info_handle = CreateFileMapping(
		INVALID_HANDLE_VALUE,
		NULL,
		PAGE_READWRITE,
		0,
		sizeof(IPC_KV_Info),
		handle_path.c_str()
	);

	if (info_handle == NULL)
	{
		throw std::runtime_error("could not create file mapping object.");
	}

	//////////////////////////////////////////////////

	bool does_already_exist = GetLastError() == ERROR_ALREADY_EXISTS;

	auto buffer = MapViewOfFile(
		info_handle,
		FILE_MAP_ALL_ACCESS,
		0,
		0,
		sizeof(IPC_KV_Info)
	);

	if (buffer == NULL)
	{
		CloseHandle(info_handle);

		throw std::runtime_error("could not map view of file.");
	}

	//////////////////////////////////////////////////

	m_controller->m_info = (IPC_KV_Info*)buffer;
	m_controller->m_info_handle = info_handle;

	//////////////////////////////////////////////////

	if (!does_already_exist)
	{
		printf("Initializing info %s...\n", handle_path.c_str());

		m_controller->m_info->m_buffer_state = false;

		m_controller->m_info->m_size[0] = 0;
		m_controller->m_info->m_size[1] = 0;

		m_controller->m_info->m_resize_count[0] = 0;
		m_controller->m_info->m_resize_count[1] = 0;

		m_controller->m_info->m_capacity[0] = INITIAL_CAPACITY;
		m_controller->m_info->m_capacity[1] = INITIAL_CAPACITY;
	}

	m_resize_count = m_controller->getResizeCount();
}

void IPC_KV::initialize_data(const std::string& name)
{
	auto allocation_size = sizeof(IPC_KV_Data) * m_controller->getCapacity();
	auto resize_count = m_controller->getResizeCount();

	///////////////////////////////////////////

	auto handle_path = "ipckv_" + std::to_string(resize_count) + "_" + name;

	if (handle_path.length() > MAX_PATH)
	{
		throw std::runtime_error("key is too long.");
	}

	///////////////////////////////////////////

	auto data_handle = CreateFileMapping(
		INVALID_HANDLE_VALUE,
		NULL,
		PAGE_READWRITE,
		0,
		allocation_size,
		handle_path.c_str()
	);

	if (data_handle == NULL)
	{
		throw std::runtime_error("could not create file mapping object.");
	}

	///////////////////////////////////////////

	auto does_already_exist = GetLastError() == ERROR_ALREADY_EXISTS;

	auto buffer = MapViewOfFile(
		data_handle,
		FILE_MAP_ALL_ACCESS,
		0,
		0,
		allocation_size
	);

	if (buffer == NULL)
	{
		CloseHandle(data_handle);

		throw std::runtime_error("could not map view of file.");
	}

	///////////////////////////////////////////

	m_controller->m_data = (IPC_KV_Data*)buffer;
	m_controller->m_data_handle = data_handle;

	///////////////////////////////////////////

	if (!does_already_exist)
	{
		printf("Initializing data %s...\n", handle_path.c_str());

		std::memset(m_controller->m_data, 0, allocation_size);
	}
}

void IPC_KV::set(const std::string& key, unsigned char* data, size_t size)
{
	auto lock = get_lock(WRITE_LOCK);

	if (size >= DATA_SIZE)
		throw std::runtime_error("data size is too big");

	if (key.length() >= KEY_SIZE - 1)
		throw std::runtime_error("key size is too big");

	set_internal(key, data, size);
}

void IPC_KV::set_internal(const std::string& key, unsigned char* data, size_t size)
{
	if (LOAD_FACTOR >= MAX_LOAD_FACTOR)
		resize();
	 
	uint32_t probeIndex = 0;
	uint32_t bucketsProbed = 0;

	uint32_t capacity = m_controller->getCapacity();

	uint32_t hashCode = hash(key.c_str(), key.length()); 
	uint32_t bucket = hashCode % capacity; 

	while (bucketsProbed < m_controller->getCapacity())
	{
		if (m_controller->getDataState(bucket) != IPC_KV_Data_State::Occupied || m_controller->getDataKey(bucket) == key)
		{ 
			if (m_controller->getDataState(bucket) != IPC_KV_Data_State::Occupied)
			{
				m_controller->startInfoTransaction();
				m_controller->setSize(m_controller->getSize() + 1);

				if (should_crash)
					exit(0);

				m_controller->commitInfo();
			}

			m_controller->startDataTransaction(bucket);
			m_controller->setDataKey(bucket, key.c_str(), key.length());
			m_controller->setData(bucket, data, size);
			m_controller->setDataState(bucket, IPC_KV_Data_State::Occupied);
			m_controller->commitData(bucket);
			
			return;
		}

		printf("Collision! %s -> %d\n", key.c_str(), bucket);

		probeIndex++;

		bucket = (hashCode + C1_CONSTANT * probeIndex + C2_CONSTANT * probeIndex * probeIndex) % capacity;
		bucketsProbed++;
	}

	throw std::runtime_error("unable to insert item due to unexpected error");
}

void IPC_KV::print()
{
	auto lock = get_lock(READ_LOCK);

	////////////////////////////////////////////////////

	auto capacity = m_controller->getCapacity();

	for (size_t i = 0; i < capacity; i++)
	{
		if (m_controller->getDataState(i) == IPC_KV_Data_State::Occupied)
			printf("[%d] %s 0x%X\n", i, m_controller->getDataKey(i), m_controller->getDataSize(i));
	}

	printf("Capacity %d, Actual Size %d, Size (1) %d, Size (2) %d, Resizes %d, Load Factor %f\n", 
		m_controller->getCapacity(), 
		m_controller->getSize(),
		m_controller->m_info->m_size[0],
		m_controller->m_info->m_size[1], 
		m_controller->getResizeCount(), 
		LOAD_FACTOR
	);
}


IPC_Lock IPC_KV::get_lock(bool is_writing)
{
	IPC_Lock lock(is_writing, m_name);

	

	return lock;
}

void IPC_KV::resize()
{
	printf("Resizing memory.\n");
}

size_t IPC_KV::size()
{
	auto lock = get_lock(READ_LOCK);

	////////////////////////////////////////////////////

	return m_controller->getSize();
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

int main()
{
	unsigned char dummy_data[] = { 0x68, 0x69 };
	auto kv = IPC_KV("test");

	while (1)
	{
		std::string key;

		getline(std::cin, key);

		if (!key.empty())
		{
			printf("Inserting %s\n", key.c_str());
			kv.set(key, dummy_data, sizeof(dummy_data));
		}
		else
		{ 
			should_crash = !should_crash;
			printf("Will crash %s\n", should_crash ? "YUP BABYE" : "no");
		}

		kv.print();
	}

	printf("Finished execution, %d\n", kv.size());

	
	Sleep(500000000);

	return 0;
}