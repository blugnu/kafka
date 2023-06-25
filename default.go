package kafka

var Default = struct {
	EncryptionHandler
	ConsumerErrorHandler OnErrorHandler
	RetryMetadataHandler
	Producer
}{
	EncryptionHandler:    NoEncryption(),
	ConsumerErrorHandler: HaltConsumer(),
	RetryMetadataHandler: DefaultRetryMetadataHandler(),
}

var defaultretrymetadatahandler = &retrymetadatahandler{}

func DefaultRetryMetadataHandler() RetryMetadataHandler {
	return defaultretrymetadatahandler
}
