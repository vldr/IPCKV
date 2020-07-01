#include "ipc_kv.h"

bool should_crash = false;

IPC_KV::IPC_KV(const std::string& name)
{
	m_name = name;
	m_controller = new IPC_KV_Controller();

	//////////////////////////////////////

	initialize_info(m_name);

	//////////////////////////////////////

	auto data_tuple = initialize_data(
		m_name, 
		m_controller->getCapacity(), 
		m_controller->getResizeCount()
	); 

	m_controller->m_data = std::get<0>(data_tuple);
	m_controller->m_data_handle = std::get<1>(data_tuple);
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
		LOG("Initializing info %s...\n", handle_path.c_str());

		m_controller->m_info->m_buffer_state = false;

		m_controller->startInfoTransaction();
		m_controller->setSize(0);
		m_controller->setResizeCount(0);
		m_controller->setCapacity(IPCKV_INITIAL_CAPACITY);
		m_controller->commitInfo();
	}

	m_resize_count = m_controller->getResizeCount();
}

std::tuple<IPC_KV_Data*, HANDLE> IPC_KV::initialize_data(const std::string& name, size_t capacity, size_t resize_count)
{
	auto allocation_size = sizeof(IPC_KV_Data) * capacity;

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

	if (!does_already_exist)
	{
		LOG("Initializing data %s...\n", handle_path.c_str());

		std::memset(buffer, 0, allocation_size);
	}

	return std::make_tuple((IPC_KV_Data*)buffer, data_handle);
}

void IPC_KV::clear()
{
	auto lock = get_lock(IPCKV_WRITE_LOCK);

	auto capacity = m_controller->getCapacity();

	for (size_t i = 0; i < capacity; i++)
	{
		if (m_controller->getDataState(i) == IPC_KV_Data_State::Occupied)
		{
			m_controller->startDataTransaction(i);
			m_controller->setDataState(i, IPC_KV_Data_State::Deleted);
			m_controller->commitData(i);

			m_controller->startInfoTransaction();
			m_controller->setSize(m_controller->getSize() - 1);
			m_controller->commitInfo();
		}
	}
}

bool IPC_KV::remove(const std::string& key)
{
	auto lock = get_lock(IPCKV_WRITE_LOCK);

	uint32_t probeIndex = 0;
	uint32_t bucketsProbed = 0;

	uint32_t capacity = m_controller->getCapacity();

	uint32_t hashCode = hash(key.c_str(), key.length());
	uint32_t bucket = hashCode % capacity;

	while (bucketsProbed < capacity)
	{
		if (m_controller->getDataState(bucket) == IPC_KV_Data_State::Empty)
		{
			return false;
		}

		if (
			m_controller->getDataState(bucket) == IPC_KV_Data_State::Occupied
			&& m_controller->getDataKey(bucket) == key
			)
		{
			m_controller->startDataTransaction(bucket);
			m_controller->setDataState(bucket, IPC_KV_Data_State::Deleted);
			m_controller->commitData(bucket);

			m_controller->startInfoTransaction();
			m_controller->setSize(m_controller->getSize() - 1);
			m_controller->commitInfo();

			return true;
		}

		probeIndex++;

		bucket = (hashCode + IPCKV_C1_CONSTANT * probeIndex + IPCKV_C2_CONSTANT * probeIndex * probeIndex) % capacity;
		bucketsProbed++;
	}

	return false;
}

bool IPC_KV::get(const std::string& key, unsigned char* data, size_t & size)
{
	auto lock = get_lock(IPCKV_READ_LOCK);

	uint32_t probeIndex = 0;
	uint32_t bucketsProbed = 0;

	uint32_t capacity = m_controller->getCapacity();

	uint32_t hashCode = hash(key.c_str(), key.length());
	uint32_t bucket = hashCode % capacity;

	while (bucketsProbed < capacity)
	{
		if (m_controller->getDataState(bucket) == IPC_KV_Data_State::Empty)
		{
			return false;
		}

		if (
			m_controller->getDataState(bucket) == IPC_KV_Data_State::Occupied 
			&& m_controller->getDataKey(bucket) == key
		)
		{
			size = m_controller->getDataSize(bucket);

			return memcpy_s(data, IPCKV_DATA_SIZE, m_controller->getData(bucket), size) == 0;
		}

		probeIndex++;

		bucket = (hashCode + IPCKV_C1_CONSTANT * probeIndex + IPCKV_C2_CONSTANT * probeIndex * probeIndex) % capacity;
		bucketsProbed++;
	}

	return false;
}

void IPC_KV::set(const std::string& key, unsigned char* data, size_t size)
{
	auto lock = get_lock(IPCKV_WRITE_LOCK);

	if (size >= IPCKV_DATA_SIZE)
		throw std::runtime_error("data size is too big");

	if (key.length() >= IPCKV_KEY_SIZE - 1)
		throw std::runtime_error("key size is too big");

	if (IPCKV_LOAD_FACTOR >= IPCKV_MAX_LOAD_FACTOR)
		resize();
	 
	uint32_t probeIndex = 0;
	uint32_t bucketsProbed = 0;

	uint32_t capacity = m_controller->getCapacity();

	uint32_t hashCode = hash(key.c_str(), key.length()); 
	uint32_t bucket = hashCode % capacity; 

	while (bucketsProbed < capacity)
	{
		if (m_controller->getDataState(bucket) != IPC_KV_Data_State::Occupied || m_controller->getDataKey(bucket) == key)
		{ 
			if (m_controller->getDataState(bucket) != IPC_KV_Data_State::Occupied)
			{
				m_controller->startInfoTransaction(); 
				m_controller->setSize(m_controller->getSize() + 1);
				m_controller->commitInfo();
			}

			m_controller->startDataTransaction(bucket);
			m_controller->setDataKey(bucket, key.c_str(), key.length());
			m_controller->setData(bucket, data, size);
			m_controller->setDataState(bucket, IPC_KV_Data_State::Occupied);
			m_controller->commitData(bucket);
			
			return;
		}

		LOG("Collision! %s -> %d\n", key.c_str(), bucket);

		probeIndex++;

		bucket = (hashCode + IPCKV_C1_CONSTANT * probeIndex + IPCKV_C2_CONSTANT * probeIndex * probeIndex) % capacity;
		bucketsProbed++;
	}

	throw std::runtime_error("unable to insert item due to unexpected error");
}

void IPC_KV::print()
{
	auto lock = get_lock(IPCKV_READ_LOCK);

	////////////////////////////////////////////////////

	auto capacity = m_controller->getCapacity();

	for (size_t i = 0; i < capacity; i++)
	{
		if (m_controller->getDataState(i) == IPC_KV_Data_State::Occupied)
			LOG("[%d] %s 0x%X\n", i, m_controller->getDataKey(i), m_controller->getDataSize(i));

		if (m_controller->getDataState(i) == IPC_KV_Data_State::Deleted)
			LOG("[%d] Deleted\n", i);
	}

	LOG("Capacity %d, Size %d, Resizes %d, Load Factor %f\n",
		m_controller->getCapacity(), 
		m_controller->getSize(),
		m_controller->getResizeCount(), 
		IPCKV_LOAD_FACTOR
	);
}
 

IPC_Lock IPC_KV::get_lock(bool is_writing)
{
	IPC_Lock lock(is_writing, m_name);
	 
	if (m_resize_count != m_controller->getResizeCount())
	{ 
		LOG("Expired memory, fetching new memory.\n");

		UnmapViewOfFile(m_controller->m_data);
		CloseHandle(m_controller->m_data_handle);

		auto data_tuple = initialize_data(
			m_name,
			m_controller->getCapacity(),
			m_controller->getResizeCount()
		);

		m_controller->m_data = std::get<0>(data_tuple);
		m_controller->m_data_handle = std::get<1>(data_tuple);
		m_resize_count = m_controller->getResizeCount();
	}

	return lock;
}

void IPC_KV::resize()
{  
	LOG("Resizing memory.\n");

	//////////////////////////////////////////

	auto new_capacity = find_nearest_prime(m_controller->getCapacity() * 2);
	auto new_resize_count = m_controller->getResizeCount() + 1;

	m_controller->startInfoTransaction();
	m_controller->setCapacity(new_capacity);
	m_controller->setResizeCount(new_resize_count);

	//////////////////////////////////////////

	IPC_KV_Controller temp_controller{};

	auto new_data_tuple = initialize_data(m_name, new_capacity, new_resize_count);
	temp_controller.m_data = std::get<0>(new_data_tuple);
	temp_controller.m_data_handle = std::get<1>(new_data_tuple);

	for (size_t i = 0; i < m_controller->getCapacity(); i++)
	{
		if (m_controller->getDataState(i) != IPC_KV_Data_State::Occupied)
			continue;  

		auto key = m_controller->getDataKey(i);
		auto keyLength = strlen(key);

		auto data = m_controller->getData(i);
		auto data_size = m_controller->getDataSize(i);

		/////////////////////////////////////////////////////

		uint32_t probeIndex = 0;
		uint32_t bucketsProbed = 0;

		uint32_t hashCode = hash(key, keyLength);
		uint32_t bucket = hashCode % new_capacity;

		while (bucketsProbed < new_capacity)
		{
			if (temp_controller.getDataState(bucket) != IPC_KV_Data_State::Occupied || temp_controller.getDataKey(bucket) == key)
			{
				temp_controller.startDataTransaction(bucket);
				temp_controller.setDataKey(bucket, key, keyLength);
				temp_controller.setData(bucket, data, data_size);
				temp_controller.setDataState(bucket, IPC_KV_Data_State::Occupied);
				temp_controller.commitData(bucket);

				break;
			}

			probeIndex++;

			bucket = (hashCode + IPCKV_C1_CONSTANT * probeIndex + IPCKV_C2_CONSTANT * probeIndex * probeIndex) % new_capacity;
			bucketsProbed++;
		}

		if (bucketsProbed >= new_capacity)
			throw std::runtime_error("unable to resize item due to unexpected error");
	}
	 
	m_controller->commitInfo();

	std::swap(m_controller->m_data, temp_controller.m_data);
	std::swap(m_controller->m_data_handle, temp_controller.m_data_handle);

	m_resize_count = new_resize_count;
}

size_t IPC_KV::size()
{
	auto lock = get_lock(IPCKV_READ_LOCK);

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

#include <random>
#include <string>

int main()
{
	auto random_string = [](size_t length) -> std::string
	{
		static auto& chrs = "0123456789"
			"abcdefghijklmnopqrstuvwxyz"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

		thread_local static std::mt19937 rg{ std::random_device{}() };
		thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

		std::string s;

		s.reserve(length);

		while (length--)
			s += chrs[pick(rg)];

		return s;
	};


	std::string title = std::to_string(GetCurrentProcessId());

	SetConsoleTitleA(title.c_str());

	unsigned char dummy_data[] = { 0x68, 0x69 };
	


	try {
		auto kv = IPC_KV("test");
		kv.set("How are you?", dummy_data, sizeof(dummy_data));
		kv.set("Hello World", dummy_data, sizeof(dummy_data));
		kv.print();

		kv.remove("Hello World");
		kv.print();

		kv.clear();
		kv.print();

		kv.set("ABC", dummy_data, sizeof(dummy_data));
		kv.set("EFG", dummy_data, sizeof(dummy_data));
		kv.set("XYZ", dummy_data, sizeof(dummy_data));
		
		kv.print();

		kv.remove("ABC");

		unsigned char dummy_callback_data[IPCKV_DATA_SIZE];
		size_t dummy_callback_size;

		printf("Finding EFG 0x%X\n", kv.get("EFG", dummy_callback_data, dummy_callback_size));

		kv.print();
		printf("Finished execution, %d\n", kv.size());
	}
	catch (std::runtime_error & ex)
	{
		printf("Exception %s LastError %X\n", ex.what(), GetLastError());
	}
	
	
	Sleep(500000000);

	return 0;
}