package kafka

type ProducerConfiguration func(*Config, *producer) error

func Encryption(handler EncryptionHandler) ProducerConfiguration {
	return func(_ *Config, producer *producer) error {
		producer.EncryptionHandler = handler
		return nil
	}
}
