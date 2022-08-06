#pragma once
enum class Types {
	Error,
	EchoRequest,
	EchoResponse,

	AddServerRequest,
	AddServerResponse,
	AddFileServerRequest,
	AddFileServerResponse,
	AddBackupServerRequest,
	AddBackupServerResponse,
	GetServerRequest,
	GetServerResponse,
	GetFileServerRequest,
	GetFileServerResponse,
	GetBackupServerRequest,
	GetBackupServerResponse,
	
	LoginRequest,
	LoginResponse,
	AddFileMetaRequest,
	AddFileMetaResponse,
	GetFileMetaRequest,
	GetFileMetaResponse,
	GetFileSroteServerRequest,
	GetFileSroteServerResponse,
	DeleteFileMetaRequest,
	DeleteFileMetaResponse,
	

	AddFileRequest,
	AddFileResponse,
	GetFileRequest,
	GetFileResponse,
	AddBackupFileRequest,
	AddBackupFileResponse,


};

enum class Errors {
	Success,
	FileNotExist,
	FileStoreNotExist,
	AuthFail,
	SendError,
	OpenFileError,
};

