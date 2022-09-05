package main

type FileManager struct {
	MessageReceiver  func(ConnectionParams interface{}) (<-chan interface{}, error)
	MessageSender    func(message interface{}, ConnectionParams interface{}) error
	MessageProcessor func(message interface{}, breaker Breaker, FM FileManager) error
	ConfigGetter     func(team string) ([]TeamConfig, error)
	ConfigSetter     func(tc TeamConfig) error
	ConfigRemover    func(tc TeamConfig) error
	ConfigProcessor  func(message interface{}, FM FileManager) error
	ConnectionParams interface{}
}

func (f FileManager) StartReceiveMessage() (<-chan interface{}, error) {
	result, err := f.MessageReceiver(f.ConnectionParams)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (f FileManager) SendMessage(message interface{}) error {
	err := f.MessageSender(message, f.ConnectionParams)
	if err != nil {
		return err
	}
	return nil
}

func (f FileManager) ProcessMessage(message interface{}, breaker Breaker) error {
	err := f.MessageProcessor(message, breaker, f)
	if err != nil {
		return err
	}
	return nil
}

func (f FileManager) GetConfig(team string) ([]TeamConfig, error) {
	configs, err := f.ConfigGetter(team)
	if err != nil {
		return nil, err
	}
	return configs, nil
}

func (f FileManager) SetConfig(tc TeamConfig) error {
	err := f.ConfigSetter(tc)
	if err != nil {
		return err
	}
	return nil
}

func (f FileManager) RemoveConfig(tc TeamConfig) error {
	err := f.ConfigRemover(tc)
	if err != nil {
		return err
	}
	return nil
}

func (f FileManager) ProcessConfig(message interface{}) error {
	err := f.ConfigProcessor(message, f)
	if err != nil {
		return err
	}
	return nil
}

func GetFileManagerOverloadInstance(
	receiver func(ConnectionParams interface{}) (<-chan interface{}, error),
	sender func(message interface{}, ConnectionParams interface{}) error,
	process func(message interface{}, breaker Breaker, FM FileManager) error,
	confGetter func(team string) ([]TeamConfig, error),
	confSetter func(tc TeamConfig) error,
	confRemover func(tc TeamConfig) error,
	confProcessor func(message interface{}, FM FileManager) error,
	connectionParams interface{},
) FileManager {
	fm := FileManager{
		MessageReceiver:  receiver,
		MessageSender:    sender,
		MessageProcessor: process,
		ConfigGetter:     confGetter,
		ConfigSetter:     confSetter,
		ConfigRemover:    confRemover,
		ConfigProcessor:  confProcessor,
		ConnectionParams: connectionParams,
	}
	return fm
}

func GetFileManagerDefaultInstance(ConnectionParams interface{}) FileManager {
	fm := FileManager{
		MessageReceiver:  ReceiveMessage,
		MessageSender:    SendMessage,
		MessageProcessor: ProcessMessage,
		ConfigGetter:     getConfig,
		ConfigSetter:     setConfig,
		ConfigRemover:    removeConfig,
		ConfigProcessor:  processConfigUpdate,
		ConnectionParams: ConnectionParams,
	}
	return fm
}
