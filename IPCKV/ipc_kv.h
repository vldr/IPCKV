#pragma once
#include <Windows.h>
#include <string>
#include <unordered_map>
#include <mutex>

#define LOG(...) printf(__VA_ARGS__)
#define MAX_LOCKS 24

#define MAX_LOAD_FACTOR 0.6f
#define INITIAL_CAPACITY 10
#define DATA_SIZE 2048
#define KEY_SIZE 260

#define LOAD_FACTOR (float)m_info->size / (float)m_info->capacity

#define C1_CONSTANT 3
#define C2_CONSTANT 5

class IPC_KV 
{
public:
	IPC_KV(const std::string& name);
	~IPC_KV();

	bool set(const std::string& key, unsigned char* data, size_t size);
	void print();
	size_t size();
private:
	enum IPC_KV_Data_State
	{
		Empty = 0,
		Deleted = 1,
		Occupied = 2,
	};

	struct IPC_KV_Info
	{
		size_t capacity;
		size_t size;

		size_t resize_count;
	};

	struct IPC_KV_Data
	{
		IPC_KV_Data_State state;

		char key[KEY_SIZE];

		unsigned char data[DATA_SIZE];
		size_t size;
	};

	size_t find_nearest_prime(size_t input);
	bool is_prime(size_t input);

	void resize();
	void check_capacity();

	uint32_t hash(const char* key, int count);

	void initialize_info(const std::string& name);
	void initialize_data(const std::string& name);

	IPC_KV_Info* m_info = nullptr;
	IPC_KV_Data* m_data = nullptr;

	HANDLE m_info_handle = nullptr;
	HANDLE m_data_handle = nullptr;	

	std::string m_name;
	std::string m_lock_path;

	size_t m_cached_resize_count;
};

class IPC_ReadLock {
public:
	IPC_ReadLock(const std::string& name)
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

	~IPC_ReadLock() noexcept
	{
		if (semaphore_handle)
		{
			ReleaseSemaphore(semaphore_handle, 1, nullptr);
			CloseHandle(semaphore_handle);
		}
	}

	IPC_ReadLock(const IPC_ReadLock&) = delete;
	IPC_ReadLock& operator=(IPC_ReadLock const&) = delete;

	IPC_ReadLock(IPC_ReadLock&& ipc_read_lock) noexcept :
		semaphore_handle(std::exchange(ipc_read_lock.semaphore_handle, nullptr)) { }

	IPC_ReadLock& operator=(IPC_ReadLock&& ipc_read_lock)
	{
		semaphore_handle = std::exchange(ipc_read_lock.semaphore_handle, nullptr);

		return *this;
	}

private:
	HANDLE semaphore_handle = nullptr;
};

class IPC_WriteLock {
public:
	IPC_WriteLock(const std::string& name)
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

	~IPC_WriteLock() noexcept
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

	IPC_WriteLock(const IPC_WriteLock&) = delete;
	IPC_WriteLock& operator=(IPC_WriteLock const&) = delete;

	IPC_WriteLock(IPC_WriteLock&& ipc_write_lock) noexcept :
		semaphore_handle(std::exchange(ipc_write_lock.semaphore_handle, nullptr)),
		mutex_handle(std::exchange(ipc_write_lock.mutex_handle, nullptr)) { }

	IPC_WriteLock& operator=(IPC_WriteLock&& ipc_write_lock)
	{
		semaphore_handle = std::exchange(ipc_write_lock.semaphore_handle, nullptr);
		mutex_handle = std::exchange(ipc_write_lock.mutex_handle, nullptr);

		return *this;
	}
private:
	HANDLE semaphore_handle = nullptr;
	HANDLE mutex_handle = nullptr;
};

struct IPC_Object
{


	//size_t size = 0;
	//uint8_t buffer[BLOCK_SIZE];
};

/*class IPC_Value {
public:
	IPC_Value(const std::string & key, const std::string& read_lock_name, IPC_Object* ipc_object)
		: m_ipc_object(ipc_object), m_read_lock(read_lock_name) {}

	IPC_Value(const IPC_Value&) = delete;
	IPC_Value& operator=(IPC_Value const&) = delete;

	IPC_Value(IPC_Value&& ipc_value) noexcept :
		m_ipc_object(std::exchange(ipc_value.m_ipc_object, nullptr)),
		m_read_lock(std::move(ipc_value.m_read_lock))
	{ }

	IPC_Value& operator=(IPC_Value&& ipc_value)
	{
		m_ipc_object = std::exchange(ipc_value.m_ipc_object, nullptr);
		m_read_lock = std::move(ipc_value.m_read_lock);

		return *this;
	}

	inline const uint8_t* buffer() {
		return m_ipc_object ? m_ipc_object->buffer : nullptr;
	}

	inline size_t size() {
		return m_ipc_object ? m_ipc_object->size : 0;
	}
private:
	IPC_Object* m_ipc_object;
	IPC_ReadLock m_read_lock;
};*/