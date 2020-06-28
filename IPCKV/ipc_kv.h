#pragma once
#include <Windows.h>
#include <string>
#include <iostream>

#define LOG(...) printf(__VA_ARGS__)
#define MAX_LOCKS 24

#define MAX_LOAD_FACTOR 0.6f
#define INITIAL_CAPACITY 10
#define DATA_SIZE 2048
#define KEY_SIZE 260

#define LOAD_FACTOR (float)m_controller->getSize() / (float)m_controller->getCapacity()

#define C1_CONSTANT 3
#define C2_CONSTANT 5

#define READ_LOCK false
#define WRITE_LOCK true

class IPC_Lock;

enum IPC_KV_Data_State
{
	Empty = 0,
	Deleted = 1,
	Occupied = 2,
};

struct IPC_KV_Data
{
	IPC_KV_Data_State m_state[2];

	char m_key[2][KEY_SIZE];
	unsigned char m_value[2][DATA_SIZE];

	size_t m_size[2];

	bool m_buffer_state;
};

struct IPC_KV_Info
{
	bool m_buffer_state;
	size_t m_capacity[2];
	size_t m_size[2];
	size_t m_resize_count[2];
};

class IPC_KV_Controller
{
public:
	IPC_KV_Controller() {}

	~IPC_KV_Controller()
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

	// Disallow copying and moving.
	IPC_KV_Controller(const IPC_KV_Controller&) = delete;
	IPC_KV_Controller(IPC_KV_Controller&&) = delete;
	IPC_KV_Controller& operator=(const IPC_KV_Controller&) = delete;
	IPC_KV_Controller& operator=(IPC_KV_Controller&&) = delete;

	/**
	* m_info Setters
	*/

	void startInfoTransaction()
	{
		setResizeCount(getResizeCount());
		setCapacity(getCapacity());
		setSize(getSize());

		has_started_info_transaction = true;
	}

	void setResizeCount(size_t resize_count)
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		m_info->m_resize_count[!m_info->m_buffer_state] = resize_count;
	}

	void setCapacity(size_t capacity)
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		m_info->m_capacity[!m_info->m_buffer_state] = capacity;
	}

	void setSize(size_t size)
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		m_info->m_size[!m_info->m_buffer_state] = size;
	}

	/** 
	* m_Data Setters
	*/

	void startDataTransaction(size_t index)
	{
		setDataSize(index, getDataSize(index));
		setDataState(index, getDataState(index));
		setData(index, getData(index), getDataSize(index));
		setDataKey(index, getDataKey(index), strlen(getDataKey(index)));

		has_started_data_transaction = true;
	}

	void setDataSize(size_t index, size_t size)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = !m_data[index].m_buffer_state;

		m_data[index].m_size[buffer_state] = size;
	}

	void setDataState(size_t index, IPC_KV_Data_State state)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = !m_data[index].m_buffer_state;

		m_data[index].m_state[buffer_state] = state;
	}

	void setData(size_t index, unsigned char* data, size_t size)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = !m_data[index].m_buffer_state;

		memcpy_s(m_data[index].m_value[buffer_state], DATA_SIZE, data, size);
		m_data[index].m_size[buffer_state] = size;
	}

	void setDataKey(size_t index, const char* key, size_t size)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = !m_data[index].m_buffer_state;

		strncpy_s(m_data[index].m_key[buffer_state], key, size);
	}

	/**
	* m_info Getters
	*/

	size_t getCapacity()
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		return m_info->m_capacity[m_info->m_buffer_state];
	}

	size_t getSize()
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		return m_info->m_size[m_info->m_buffer_state];
	}

	size_t getResizeCount()
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		return m_info->m_resize_count[m_info->m_buffer_state];
	}

	/**
	* m_data Getters
	*/

	unsigned char* getData(size_t index)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = m_data[index].m_buffer_state;

		return m_data[index].m_value[buffer_state];
	}

	size_t getDataSize(size_t index)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = m_data[index].m_buffer_state;

		return m_data[index].m_size[buffer_state];
	}

	IPC_KV_Data_State getDataState(size_t index)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = m_data[index].m_buffer_state;

		return m_data[index].m_state[buffer_state];
	}

	char* getDataKey(size_t index)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		bool buffer_state = m_data[index].m_buffer_state;

		return m_data[index].m_key[buffer_state];
	}

	/**
	* Commit Info
	*/
	void commitInfo() 
	{
		if (!m_info)
			throw std::runtime_error("class is in an invalid state.");

		if (!has_started_info_transaction)
			throw std::runtime_error("a info transaction has not been started.");

		m_info->m_buffer_state = !m_info->m_buffer_state;
	}
	/**
	* Commit Data
	*/
	void commitData(size_t index)
	{
		if (!m_data)
			throw std::runtime_error("class is in an invalid state.");

		if (!has_started_data_transaction)
			throw std::runtime_error("a data transaction has not been started.");

		m_data[index].m_buffer_state = !m_data[index].m_buffer_state;
	}

	bool has_started_info_transaction = false;
	bool has_started_data_transaction = false;

	IPC_KV_Info* m_info = nullptr;
	IPC_KV_Data* m_data = nullptr;

	HANDLE m_info_handle = nullptr;
	HANDLE m_data_handle = nullptr;
};

class IPC_KV 
{
public:
	/**
	* Constructors and destructors
	*/
	IPC_KV(const std::string& name);
	~IPC_KV();

	/**
	* Public Methods
	*/
	void set(const std::string& key, unsigned char* data, size_t size);
	void print();
	size_t size();
private:
	/**
	* Private Methods
	*/
	void initialize_info(const std::string& name);
	void initialize_data(const std::string& name);
	void resize();
	void set_internal(const std::string& key, unsigned char* data, size_t size);
	bool is_prime(size_t input);

	size_t find_nearest_prime(size_t input);
	uint32_t hash(const char* key, int count);
	IPC_Lock get_lock(bool is_writing);

	/**
	* Private Members
	*/
	IPC_KV_Controller* m_controller = nullptr;
	std::string m_name;
	size_t m_resize_count;
};

class IPC_Lock {
public:
	IPC_Lock(bool is_write_lock, const std::string& name)
	{
		if (is_write_lock)
		{
			if (name.length() > MAX_PATH)
			{
				throw std::runtime_error("rwlock name too long.");
			}

			auto mutex_name = name + "_mutex";

			mutex_handle = CreateMutexA(
				nullptr,
				FALSE,
				mutex_name.c_str()
			);

			if (mutex_handle == nullptr)
			{
				throw std::runtime_error("could not create mutex.");
			}

			auto wait_result = WaitForSingleObject(mutex_handle, INFINITE);

			if (wait_result != WAIT_OBJECT_0)
				throw std::runtime_error("failed to wait for semaphore object");

			//////////////////////////////////////////////

			semaphore_handle = CreateSemaphoreA(
				nullptr,
				0,
				MAX_LOCKS,
				name.c_str()
			);

			if (semaphore_handle == nullptr)
			{
				throw std::runtime_error("could not create rwlock.");
			}

			if (GetLastError() == ERROR_ALREADY_EXISTS)
			{
				for (int i = 0; i < MAX_LOCKS; i++)
				{
					auto wait_result = WaitForSingleObject(semaphore_handle, INFINITE);

					if (wait_result != WAIT_OBJECT_0)
						throw std::runtime_error("failed to wait for semaphore object");
				}
			}
		}
		else
		{
			if (name.length() > MAX_PATH)
			{
				throw std::runtime_error("rwlock name too long.");
			}

			semaphore_handle = CreateSemaphoreA(
				nullptr,
				MAX_LOCKS,
				MAX_LOCKS,
				name.c_str()
			);

			if (semaphore_handle == nullptr)
			{
				throw std::runtime_error("could not create rwlock.");
			}

			auto wait_result = WaitForSingleObject(semaphore_handle, INFINITE);

			if (wait_result != WAIT_OBJECT_0)
				throw std::runtime_error("failed to wait for single object");
		}

	}

	~IPC_Lock() noexcept
	{
		if (semaphore_handle)
		{
			ReleaseSemaphore(semaphore_handle, MAX_LOCKS, nullptr);
			CloseHandle(semaphore_handle);
		}

		if (mutex_handle)
		{
			ReleaseMutex(mutex_handle);
			CloseHandle(mutex_handle);
		}
	}

	IPC_Lock(const IPC_Lock&) = delete;
	IPC_Lock& operator=(IPC_Lock const&) = delete;

	IPC_Lock(IPC_Lock&& ipc_write_lock) noexcept :
		semaphore_handle(std::exchange(ipc_write_lock.semaphore_handle, nullptr)),
		mutex_handle(std::exchange(ipc_write_lock.mutex_handle, nullptr)) { }

	IPC_Lock& operator=(IPC_Lock&& ipc_write_lock)
	{
		semaphore_handle = std::exchange(ipc_write_lock.semaphore_handle, nullptr);
		mutex_handle = std::exchange(ipc_write_lock.mutex_handle, nullptr);

		return *this;
	}
private:
	HANDLE semaphore_handle = nullptr;
	HANDLE mutex_handle = nullptr;
};