package Conf;

# Пример конфиг-файла для SMPP демона

use strict;
use vars qw(%Conf $base);

$base = '~';
my $name = 'smppae';

%Conf = (
    DB => ['dbi:Pg:dbname=database_name;user=user_name'],

    RABBITMQ => {
        host => 'localhost',
        port => 5672,
        user => 'user_name',
        password => "password",
        vhost => 'virtual_host',
        prefetch_count => 100, # это по-умолчанию
        debug => 1,
        exchange => 'exchange_name',
        expiration => 86400, # expiration при publish по-умолчанию
        reconnect_timeout => 10,
    },

    SMPP => {
        log_prefix => "$base/logs/smpp",
        pid_prefix => "$base/tmp/smpp",
        debug => 1,
        enquire_link_interval => 60,
        enquire_link_timeout => 120,
        check_connection_interval => 10,
        save_interval => 60,
        hibernate_path => "$base/spool/smpp/hibernate",
        submit_sm_resp_interval => 60,
    },

    # Очереди
    Actors => {
        Smpp_1 => {
            comment => 'SMS',
            produce => {
                '^smpp.1.*' => { comment => 'Входящие SMS', priority => 100, expiration => 0, },
                '^service.1.*' => { comment => 'Служебные submit_sm, submit_sm_resp, deliver_sm', priority => 50, expiration => 0, },
            },
            consume => [
                'free.#',
            ],
        },
        Smpp_2 => {
            comment => 'USSD',
            produce => {
                '^smpp.2.*' => { comment => 'Входящие USSD от абонентов', priority => 100, expiration => 0, },
                '^service.2.*' => { comment => 'Служебные submit_sm, submit_sm_resp, deliver_sm', priority => 50, expiration => 0, },
            },
            consume => [
                'ussd.#',
            ],
        },
    },
);
