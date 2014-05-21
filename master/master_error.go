package master

type FileAlreadyExistError struct {

}

func (e *FileAlreadyExistError) Error() string{
	return "File already exists!"
}
