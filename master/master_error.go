package master

type FileAlreadyExistError struct {

}

type FileNotExistError struct {

}

type NotEnoughAliveServer struct {

}

func (e *FileAlreadyExistError) Error() string{
	return "File already exists!"
}

func (e *FileNotExistError) Error() string {
	return "File does not exist!"
}

func (e *NotEnoughAliveServer) Error() string {
	return "Not enough alive server"
}
