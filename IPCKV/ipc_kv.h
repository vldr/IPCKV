#pragma once
#include <Windows.h>
#include <string>
#include <unordered_map>
#include <mutex>

#define LOG(...) printf(__VA_ARGS__)
#define BLOCK_SIZE 4096
#define MAX_LOCKS 24

class IPC_KV {
public:
#define MAX_LOCKS 2

	class IPC_ReadLock {
	public:
		IPC_ReadLock(const IPC_ReadLock&) = delete;
		IPC_ReadLock& operator=(IPC_ReadLock const&) = delete;

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

	private:
		HANDLE semaphore_handle = nullptr;
		HANDLE mutex_handle = nullptr;
	};

	struct IPC_Object 
	{
		size_t size = 0;
		uint8_t buffer[BLOCK_SIZE];
	};

	class IPC_Value {
	public:
		IPC_Value(const std::string & key, IPC_Object* ipc_object) 
			: m_ipc_object(ipc_object), m_read_lock(key) {}
	private:
		IPC_Object* m_ipc_object;
		IPC_ReadLock m_read_lock;
	};

	IPC_KV(std::string name);
	~IPC_KV();

	IPC_KV::IPC_Value get(const std::string&);
	void set(const std::string& key, const std::string& value);
	void set(const std::string&, const uint8_t*, const size_t);

	void destroy();
	void clear();

	IPC_KV(const IPC_KV&) = delete;
	IPC_KV(IPC_KV&&) = delete;

	IPC_KV& operator=(const IPC_KV&) = delete;
	IPC_KV& operator=(IPC_KV&&) = delete;

	std::string m_name{};
	std::string m_handle_prefix{};
	std::string m_lock_prefix{};

	std::mutex m_mutex;
private:
	IPC_Object* get_handle(const std::string & key);

	std::unordered_map<std::string, std::pair<IPC_Object*, void*>> m_buffers{};

	void* m_clear_event_handle;
	void* m_clear_wait_handle;

	
};