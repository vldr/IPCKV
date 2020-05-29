#pragma once
#include <Windows.h>
#include <string>
#include <unordered_map>

#define LOG(...) printf(__VA_ARGS__)
#define BLOCK_SIZE 4096

class IPC_KV {
public:
	struct IPC_Object
	{
		size_t size = 0;
		uint8_t buffer[BLOCK_SIZE];
	};

	IPC_KV(std::string name) : 
		m_name(std::move(name)), 
		m_handle_prefix("Global\\IPCKV_" + m_name + "_"),
		m_mutex_prefix("IPCKV_" + m_name)
	{}

	~IPC_KV();

	IPC_Object* get(const std::string&);
	bool set(const std::string&, const uint8_t*, const size_t);

	IPC_KV(const IPC_KV&) = delete;
	IPC_KV(IPC_KV&&) = delete;

	IPC_KV& operator=(const IPC_KV&) = delete;
	IPC_KV& operator=(IPC_KV&&) = delete;

	
	std::string m_name{};
	std::string m_handle_prefix{};
	std::string m_mutex_prefix{};
private:
	IPC_Object* get_handle(const std::string & key);

	std::unordered_map<std::string, void*> m_handles{};
	std::unordered_map<std::string, IPC_Object*> m_buffers{};


};