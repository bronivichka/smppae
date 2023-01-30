CREATE TABLE smpp_config (
    smpp_config_id serial NOT NULL primary key,
    name text,
    active boolean DEFAULT false NOT NULL,
    server text,
    port text,
    version integer DEFAULT 52,
    login text,
    system_type text,
    password text,
    addr_ton boolean DEFAULT false,
    addr_npi boolean DEFAULT false,
    source_addr_ton boolean DEFAULT false,
    source_addr_npi boolean DEFAULT false,
    send_rate integer,
    last_message_id bigint,
    incoming_file_state integer,
    interface text,
    note text,
    payload boolean DEFAULT true NOT NULL,
    delivery_status_mode text,
    delivery_message_field text,
    delivery_status_regexp text,
    delivery_status_code_ok integer DEFAULT 2 NOT NULL,
    delivery_message_id_operation text DEFAULT 'hex_to_dec'::text NOT NULL,
    data_coding integer DEFAULT 8 NOT NULL,
    ucs2 boolean DEFAULT true NOT NULL,
    max_udh_len integer DEFAULT 160 NOT NULL,
    max_udh_ucs2_len integer DEFAULT 70 NOT NULL,
    registered_delivery boolean DEFAULT true NOT NULL,
    service_type text,
    send_rate_period integer DEFAULT 1 NOT NULL,
    udh boolean DEFAULT true NOT NULL,
    max_payload_len integer DEFAULT 65535 NOT NULL,
    rabbitmq_exchange integer,
    submit_sm_resp_timeout integer DEFAULT 300 NOT NULL,
    deliver_sm_timeout integer DEFAULT 86400 NOT NULL,
    submit_sm_retry_status integer[],
    throttle_error_timeout integer DEFAULT 0 NOT NULL
);


COMMENT ON TABLE smpp_config IS 'Подключения по smpp';
COMMENT ON COLUMN smpp_config.smpp_config_id IS 'Идентификатор подключения';
COMMENT ON COLUMN smpp_config.name IS 'Название';
COMMENT ON COLUMN smpp_config.active IS 'Активно да/нет';
COMMENT ON COLUMN smpp_config.server IS 'Имя или ip сервера';
COMMENT ON COLUMN smpp_config.port IS 'Порт для подключения';
COMMENT ON COLUMN smpp_config.version IS 'Версия протокола smpp';
COMMENT ON COLUMN smpp_config.login IS 'Логин';
COMMENT ON COLUMN smpp_config.password IS 'Пароль';
COMMENT ON COLUMN smpp_config.send_rate IS 'Максимальная скорость отправки сообщений, в секунду';
COMMENT ON COLUMN smpp_config.last_message_id IS 'Счетчик номера исходящего сообщения';
COMMENT ON COLUMN smpp_config.incoming_file_state IS 'Идентификатор файла из таблицы smpp_file_state, в который пишем входящие события, не используется в AE';
COMMENT ON COLUMN smpp_config.interface IS 'Интерфейс, с которого открываем подключение к серверу';
