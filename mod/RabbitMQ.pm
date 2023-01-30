package RabbitMQ;

use strict;
#use warnings;
#use utf8;
#use open qw(:std :utf8);
#use open ':encoding(utf8)';

use Time::HiRes;
use Data::Dumper;
use Log;
use Conf;

use AnyEvent;
use AnyEvent::RabbitMQ;

=item $self->new($qname, $args)

Передаем имя очереди (Logic, Bulk и тд)
И хэш аргументов $args
Сейчас там используются:
on_consume_cb => sub {} - callback, который будет вызываться на каждый on_consume
consume => 1|0, по-умолчанию 1 - подписываться на очереди или нет, по-умолчанию - подписываемся

Возвращаем $self если все ОК или undef в случае ошибы

=cut
sub new {
    my ($proto, $qname, $args) = @_;

    my $class = ref($proto) || $proto;

    # states:
    # 0 | undef - не приконнекчены
    # 1 - приконнекчены
    # 2 - коннектимся
    my $self  = {
        state => 0,
        rabbitmq => undef,
        # Сколько мы имеем подписанных очередей
        consumed_count => 0,
        # Сколько очередей имеют consume = 1 параметр
        to_consume => 0,
        queue => [],
        # callbacks
        # хэш массивов колбеков на всякие события
        # используется на событие on_cancel в consume
        cbs => {},
    };

    # Настройки RabbitMQ 
    %{$self->{conf}} = %{$Conf::Conf{RABBITMQ}} or return;

    $self->{logger} = Log->new({ nologfile => 1, debug => $self->{conf}->{debug}, log_prefix => 'RabbitMQ', }) or return;

    bless ($self, $class);

    # Получим конфиг
    $self->get_config($qname, $args) or return;

    $self->connect_to_rabbitmq();

    return $self;
} # new

=item $self->get_config($qname, $args)

В Conf.pm берем:
Общие настройки RabbitMQ коннекта RABBITMQ
Очереди из Acotrs
Строим такую структуру
$self->{conf} = {
    host => ...,
    port => ...,
    user => ...,
    password => ...,
    vhost => ...,
    prefetch_count => ...,
    exchange => { name => ..., expiration => ..., type => ..., },
    queue => [], # массив очередей, которые создаем и подписываемся/не подписываемся
    route => [], # маршруты конкретно данной очереди (заданной в new первым параметром, типа Logic, Bulk и тд)
};

=cut
sub get_config {
    my ($self, $qname, $args) = @_;

    # callback на приход сообщения из очереди
    $self->{conf}->{on_consume_cb} = delete $args->{on_consume_cb} || sub {
        my ($self, $msg) = @_;
        $self->{logger}->log('DEBUG', "on_consume_cb: got message: delivery_tag $msg->{delivery_tag}", %{$msg->{headers}}, $msg->{payload});
    };

    # Режим работы: 1 - подписываемся на очередь, 0 - не подписываемся
    # По-умолчанию - подписываемся
    $self->{conf}->{consume} = defined $args->{consume} ? $args->{consume} : 1;

    # exchange и время жизни сообщения по-умолчанию
    $self->{conf}->{exchange} = { name => delete $self->{conf}->{exchange}, type => 'topic', expiration => $self->{conf}->{expiration}, };

    # Очереди, маршруты
    $self->{conf}->{queue} = [];
    $self->{conf}->{route} = [];

    # Очереди, описанные в Conf.pm
    my $qlist = $Conf::Conf{Actors} or return;

    # Сначала пройдемся по всем очередям, создадим их, прибиндим и тд
    for my $q (keys %$qlist) {
        my $queue = $qlist->{$q};

        # route - массив паттернов возможных маршрутных ключей (но только по заданной очереди)
        if ($q eq $qname and $queue->{produce} and ref($queue->{produce}) eq 'HASH') {
            for my $pattern (keys %{$queue->{produce}}) {
                push @{$self->{conf}->{route}}, { pattern => $pattern, expiration => $queue->{produce}->{$pattern}->{expiration}, priority => $queue->{produce}->{$pattern}->{priority}, };
            }
        }
    
        # Мы создаем только те очереди, на которые есть подписчики
        next unless $queue->{consume} && ref($queue->{consume}) eq 'ARRAY' && @{$queue->{consume}};

        # Это просто счетчик
        $self->{to_consume} ++ if $q eq $qname;

        push @{$self->{conf}->{queue}}, { 
            name => $q,
            route => $queue->{consume},
            consume => $q eq $qname ? 1 : 0,
            consumer_tag => $q,
            on_consume_cb => sub { $self->on_consume_cb(@_); },
        };  
    }   

#    $self->{logger}->log('DEBUG', "get_config:", Dumper($self->{conf}));
    return $self->_check_config();

} # get_config

=item $self->_check_config()

Проверяем, что у нас заданы параметры коннекта к RabbitMQ
Проверяем, что у нас есть хотя бы один exchange, и что в exchange есть очереди - хотя... ?

=cut
sub _check_config {
    my ($self) = @_;

    for my $key (qw(host port user password vhost reconnect_timeout)) {
        unless (defined $self->{conf}->{$key}) {
            $self->{logger}->log('ERROR', "check_config: empty parameter $key!");
            return 0;
        }
    }

    # Не задан exchange
    defined $self->{conf}->{exchange}->{name} or do {
        $self->{logger}->log('ERROR', "check_config: empty exchange name");
        return 0;
    };

    # Нет очередей (вообще)
    @{$self->{conf}->{queue}} or do {
        $self->{logger}->log('ERROR', "check_config: empty queue list");
        return 0;
    };

    return 1;
} # _check_config

=item $self->connect_to_rabbitmq()

Коннектимся к кролику
Процес последовательно включает в себя
connect()
open_channel
declare_exchange
declare_queue
consume on queue - если $self->{conf}->{consume}
Только после успешного consume считаем, что мы приконнектились и готовы работать

=cut
sub connect_to_rabbitmq {
    my ($self) = @_;

    # Уже приконнектились или в процессе
    return $self->{state} if $self->{state};

    $self->{logger}->log('NOTICE', "connect_to_rabbitmq: connecting to rabbitmq server with parameters:", %{$self->{conf}});

    # Коннектимся
    $self->{state} = 2;

    $self->{ar} = undef;
    $self->{rabbitmq} = undef;
    $self->{ar} = AnyEvent::RabbitMQ->new()->load_xml_spec()->connect(
        host       => $self->{conf}->{host},
        port       => $self->{conf}->{port},
        user       => $self->{conf}->{user},
        pass       => $self->{conf}->{password},
        vhost      => $self->{conf}->{vhost},
        timeout    => 15,
        tls        => 0, # Or 1 if you'd like SSL
        # Приконнектились
        on_success => sub {
            my $ar = shift;
            $self->{logger}->log("NOTICE", "connect_to_rabbitmq: Successfully connected to rabbitmq server");
            $self->rabbitmq_connected_cb($ar);
        },
        on_failure => sub {
            my ($ar, undef, $err_str) = @_;
            $self->{logger}->log('ERROR', "connect_to_rabbitmq: unable to connect to rabbitmq host $self->{conf}->{host} port $self->{conf}->{port} with user $self->{conf}->{user}: $err_str");
            $self->set_rabbitmq_disconnected_state();
        },
        on_read_failure => sub {
            my ($err_str) = @_;
            $self->{logger}->log('ERROR', "rabbitmq read failure: $err_str");
            #$self->set_rabbitmq_disconnected_state();
        },
        on_read_timeout => sub {
            $self->{logger}->log('ERROR', "rabbitmq read timeout: ", Dumper(@_));
            $self->set_rabbitmq_disconnected_state();
        },
        on_return => sub {
            my $frame = shift;
            $self->{logger}->log('ERROR', "rabbitmq unable to deliver message", Dumper($frame));
        },
        on_close => sub {
            my $why = shift;
            if (ref($why)) {
                my $method_frame = $why->method_frame;
                $self->{logger}->log('ERROR', "rabbitmq closed connection ", $method_frame->reply_code, ": ", $method_frame->reply_text);
                $self->set_rabbitmq_disconnected_state();
            } else {
                $self->{logger}->log('ERROR',"rabbitmq closed connection $why");
                $self->set_rabbitmq_disconnected_state();
            }
        },
    ); # AnyEvent::RabbitMQ->connect;
}

=item $self->put_in_queue($route, $msg, $args)

Отправка сообщения в очередь RabbitMQ
$route - маршрутный ключ (обязательный)
$msg - сообщение в виде
{
    payload => '...',
    headers => {
        from => ...,
        to => ...,
        ...
        ...
    },
}

$args - необязательные параметры:
on_ack_cb => sub {};
on_nack_cb => sub {};
expiration => ..., # по-умолчанию берем по маршрутному ключу, или дефолтный из RABBITMQ
priority => ..., # по-умолчанию берем по маршрутному ключу или 0

=cut
sub put_in_queue {
    my ($self, $route, $msg, $args) = @_;

    # Если мы не в состоянии коннекта, положим сообщение в очередь
    $self->{state} == 1 or do {
        $self->{logger}->log('ERROR', "put_in_queue: not connected state $self->{state}, saving message in local queue");
        push @{$self->{queue}}, [$route, $msg, $args];
        return;
    };

    $args ||= {};

    # Необазятельный callbacks - что делать на получение ack и nack от RabbitMQ
    $args->{on_ack_cb} ||= sub {};
    $args->{on_nack_cb} ||= sub { $self->{logger}->log('ERROR', "put_in_queue: message $msg->{headers}->{message_id} not in queue", @_); };

    # По маршрутному ключу определяем приоритет и expiration
    # В случае, если не заданы напрямую в $args
    defined($args->{expiration}) and defined($args->{priority}) or do {
        for my $r (@{$self->{conf}->{route}}) {
            next unless $route =~ /$r->{pattern}/;
            # Нашли
            $args->{expiration} = $r->{expiration} unless defined $args->{expiration};
            $args->{priority} = $r->{priority} unless defined $args->{priority};
        }

        # Не нашли
        # Выставляем по-умолчанию
        defined($args->{expiration}) or $args->{expiration} = $self->{conf}->{exchange}->{expiration};
        defined($args->{priority}) or $args->{priority} = 0;
    };

    # Если есть в хидерах параметр smpp_stat_consumer, добавим его в конец маршрута
    defined($msg->{headers}->{smpp_stat_consumer}) && ($route .= ".$msg->{headers}->{smpp_stat_consumer}");

    # event_type - если не задан в headers, выставим в маршрутный ключ
    defined($msg->{headers}->{event_type}) or $msg->{headers}->{event_type} = $route;

    # AMPQ::Common не терпит undef значений в headers
    # Уже терпит (патч)
    #$self->define_hash_val($msg->{headers});

    # Шлем сообщение в очередь
    $self->{logger}->log('DEBUG', "put_in_queue: $route", $self->{conf}->{exchange}->{name}, "headers", %{$msg->{headers}}, "payload", $msg->{payload}, "args", %$args);

    $self->{rabbitmq}->publish(
        body => $msg->{payload},
        exchange => $self->{conf}->{exchange}->{name},
        routing_key => $route,
        header => {
            # Это для того, чтобы после рестарта брокера сообщение не потерялось
            delivery_mode => 2,
            content_type => 'text/plain',
            expiration => $args->{expiration}*1000 || undef,
            priority => $args->{priority},
            headers => $msg->{headers},
        },
        on_ack => $args->{on_ack_cb},
        on_nack => $args->{on_nack_cb},
    );
} # put_in_queue

=item $self->define_hash_val($hash)

Обвязка, вызываемая со ссылкой на хэш
Проходимся по хэшу и все undef превращаем в ''
Нужно, елси непатченный Net::AMPQ::Common и в headers есть undef'ы

=cut
sub define_hash_val {
    my ($self, $hash) = @_;

    for my $h (keys %$hash) {
        if (ref $hash->{$h} eq 'HASH') {
            $self->define_hash_val($hash->{$h});
        } elsif (!ref $hash->{$h}) {
            $hash->{$h} = '' unless defined $hash->{$h};
        }
    }
} # define_hash_val

=item $self->rabbitmq_connected_cb($ar)

Функция вызывается на событие on_success вызова connect()

=cut
sub rabbitmq_connected_cb {
    my $self = shift;
    my $ar = shift;
    $ar->open_channel(
        on_success => sub {
            $self->{logger}->log('DEBUG', "open_channel: Successfully opened channel");
            # Здесь мы получили объект AnyEvent::RabbitMQ::Channel
            # Дальше читать и писать в очередь будем через него
            $self->{rabbitmq} = shift;

            # Переводим канал в режим с подтверждениями доставки
            $self->{rabbitmq}->confirm;

            # Устанавливаем лимит неподтвержденных сообщений, после которого мы перестаем их читать
            # Лимит индивидуальный на подписчика
            $self->{rabbitmq}->qos( prefetch_count => $self->{conf}->{prefetch_count}, global => 0 );

            # Создадим exchange
            $self->declare_exchange($self->{conf}->{exchange});

        }, # RabbitMQ open_channel success
        on_failure => sub {
            $self->{logger}->log('ERROR', "open_channel failure: ", Dumper(@_));
            $self->set_rabbitmq_disconnected_state();
        },
        on_close   => sub {
            my $method_frame = shift->method_frame;
            $self->{logger}->log('ERROR', "open_chanel closed channel ", $method_frame->reply_code, ": ", $method_frame->reply_text);
            $self->set_rabbitmq_disconnected_state();
        },
        on_return => sub {
            $self->{logger}->log('ERROR', "open_channel channel return: ", Dumper(@_));
            $self->set_rabbitmq_disconnected_state();
        },
    ); # open_channel
} # rabbitmq_connected_cb

=item $self->declare_exchange($exchange)

Определяем exchange
Если все ОК, на каждую очередь (на которую в проекте хоть кто-то подписывается)
вызываем declare_queue

=cut
sub declare_exchange {
    my ($self, $exchange) = @_;

    return unless %$exchange;
    $self->{logger}->log('DEBUG', "declare_exchange:", %$exchange);

    $self->{rabbitmq}->declare_exchange(
        exchange   => $exchange->{name},
        durable => 1,
        type => $exchange->{type},
        on_success => sub {
            $self->{logger}->log('NOTICE', "declare_exchange: successfully declared exchange $exchange->{name} $exchange->{type}");

            # Создаем необходимые очереди в данном направлении
            for my $queue (@{$self->{conf}->{queue}}) {
                $self->declare_queue($queue);
            }
        }, # declare_exchange on_success
        on_failure => sub {
            $self->{logger}->log('ERROR', "declare_exchange $exchange->{name} $exchange->{type} error: ", Dumper(@_));
            $self->set_rabbitmq_disconnected_state();
        },
    ); # declare_exchange
} # declare_exchange

=item $self->declare_queue($queue)

Создаем очередь
Если ОК - биндим ее с заданными маршрутными ключами

=cut
sub declare_queue {
    my ($self, $queue) = @_;
    
    $self->{logger}->log('DEBUG', "declare_queue: creating queue", map {"$_=$queue->{$_}"} keys %$queue);
    $self->{rabbitmq}->declare_queue(
        queue => $queue->{name},
        durable => 1,
        arguments => {
            'x-max-priority' => 255,
        },
        on_success => sub {
            # Здесь мы получили объект Net::AMQP::Frame::Method
            # В нем method_frame Net::AMQP::Protocol::Queue::DeclareOk
            my $mf = shift->method_frame;

            $self->{logger}->log('NOTICE', "declare_queue: created queue", $queue->{name}, "consumers:", $mf->consumer_count, "messages:", $mf->message_count);

            $self->bind_queue($queue);

        }, # declare_queue on_success
        on_failure => sub {
            $self->{logger}->log('ERROR', "declare_queue $queue->{name} error", Dumper(@_));
            $self->set_rabbitmq_disconnected_state();
        },
    ); # declare_queue
} # declare_queue

=item $self->bind_queue($queue)

Биндим очередь к exchange

=cut
sub bind_queue {
    my ($self, $queue) = @_;

    # Прицепим очередь к exchange
    for my $route (@{$queue->{route}}) {
        $self->{rabbitmq}->bind_queue(
            queue => $queue->{name},
            exchange => $self->{conf}->{exchange}->{name},
            routing_key => $route,
            on_success => sub {
                $self->{logger}->log('NOTICE', "declare_queue: successfully bound queue $queue->{name} to exchange $self->{conf}->{exchange}->{name} key $route");
                # Выставляем рабочее состояние rabbitmq: 
                # Оно должно стать == 1, когда все очереди, на которые нам надо подписаться, подписаны 
                # Подпишемся на очередь, если нужно
                # Здесь смотрим также глобальную переменную $self->{conf}->{consume}
                $self->consume_queue($queue) if $queue->{consume} and $self->{conf}->{consume} and !$queue->{consumed};
                $self->set_rabbitmq_work_state() unless $queue->{consume};
            },
            on_failure => sub {
                $self->{logger}->log('ERROR', "declare_queue: unable to bind queue $queue->{name} to exchange $self->{conf}->{exchange}->{name} key $route:", Dumper(@_));
                $self->set_rabbitmq_disconnected_state();
            },
        );
    }
} # bind_queue

=item $self->consume_queue($queue)

Подписываемся на очередь

=cut
sub consume_queue {
    my ($self, $queue, $cb) = @_;

    # Нечего делать, если мы не в стадии коннекта
    #return unless $self->{state} == 2;

    # Уже подписаны или в процессе
    return if $queue->{consumed};

    # У этой очереди выставлено consume = 0
    return unless $queue->{consume};

    # Мы в режиме "только шлем"
    return unless $self->{conf}->{consume};

    $self->{logger}->log('DEBUG', "consume_queue: consuming to", join(', ', map {"$_=$queue->{$_}"} keys %$queue));

    # В процессе
    $queue->{consumed} = 2;

    # Подписываемся на очередь
    $self->{rabbitmq}->consume(
        queue => $queue->{name},
        consumer_tag => $queue->{consumer_tag},
        no_ack => 0,
        on_failure => sub {
            my ($msg) = @_;
            $self->{logger}->log('ERROR', "consume_queue: unable to consume to queue $queue->{name} with consumer tag $queue->{consumer_tag}: $msg");
            $self->set_rabbitmq_disconnected_state();
        },
        on_success => sub {
            # Выставим флаг, что мы определили все очереди, подписались, и можем работать с кроликом
            $queue->{consumed} = 1;

            # Кол-во очередей, на которые мы подписаны
            $self->{consumed_count} ++;

            $self->{logger}->log("NOTICE", "consume_queue: on_success_event: consumed to queue $queue->{name} with consumer_tag $queue->{consumer_tag} total consumed $self->{consumed_count} to consume $self->{to_consume}");
            $self->set_rabbitmq_work_state();

            # Если есть какой-то колбек, выполним его
            $cb->() if $cb and ref($cb) eq 'CODE';
            
        },
        on_consume => sub {
            $queue->{on_consume_cb}->(@_);
        },
        # Выставим очереди состояние "не подписыаны"
        # уменьшим общее кол-во подписанных очередей на 1
        # возьмем из очереди колбеков один и выполним
        on_cancel => sub {
            $self->{logger}->log('ERROR', "consume_queue: on_cancel event: consumption to $queue->{name}, cancelled!");
            $queue->{consumed} = 0;
            $self->{consumed_count} --;

            # callback, если есть
            return unless exists $self->{cbs}->{on_success_cancel} and ref($self->{cbs}->{on_success_cancel}) eq 'ARRAY';
            my $cb = shift @{$self->{cbs}->{on_success_cancel}} or return;
            $cb->();
        }
    ); # consume
} # consume_queue

=item $self->set_rabbitmq_work_state()

Меняем состояние rabbitmq на 1 (рабочее), если подписаны на все очереди, на которые должны быть подписаны
Это в том случае, если мы вообще в режиме подписки

=cut
sub set_rabbitmq_work_state {
    my ($self) = @_;

    return unless $self->{state} == 2;

    my $state = 1;

    if ($self->{conf}->{consume}) {
        for my $queue (@{$self->{conf}->{queue}}) {
            if ($queue->{consume} and $queue->{consumed} != 1) {
                $state = 0;
                last;
            }
        }
    }

    $self->{state} = $state if $state;

    # Отправим локальную очередь, если есть
    $self->{state} == 1 and do {
        while (my $msg = shift(@{$self->{queue}})) {
            $self->put_in_queue(@$msg);
        };
    };
} # set_rabbitmq_work_state

=item $self->set_rabbitmq_disconnected_state

Выставляем $self->{state}->{rabbitmq} в undef
И заодно всем очередям выставляем consumed = false
Это касается только исходящих очередей, на которые мы подписываемся
Происходит это при ошибках в соеlинении
Далее инициируем реконнект

=cut
sub set_rabbitmq_disconnected_state {
    my ($self) = @_;

    $self->{state} = 0;
    for my $queue (@{$self->{conf}->{queue}}) {
        next unless $queue->{consumed};
        $queue->{consumed} = 0;
    }

    $self->{consumed_count} = 0;

    $self->{conn_tm} = AnyEvent->timer(
        after => $self->{conf}->{reconnect_timeout},
        cb => sub {
            $self->connect_to_rabbitmq();
        },
    );

} # set_rabbitmq_disconnected_state

=item $self->cancel($cb)

Отписываемся ото всех очередей
Функция передсат эти параметры в $self->cancel_queue()
Как только отписались ото всех очередей, вызываем необязательный $cb

=cut
sub cancel {
    my $self = shift;
    
    $self->{conf}->{consume} = 0;
    for my $queue (@{$self->{conf}->{queue}}) {
        $self->cancel_queue($queue, @_);
    }
} # cancel

=item $self->cancel_queue($queue, $ok_cb)

Отписываемся от определенной очереди
$rabbitmq->cancel_queue($queue, $ok_cb)
Можно передать в качестве необязательного параметра
$ok_cb = sub {}
Функция выполнится после успешной отписки ото всех очередей
Сделано через массив, тк основные действия производятся на on_consume в consume_queue

=cut
sub cancel_queue {
    my ($self, $queue, $ok_cb) = @_;

    $ok_cb = undef unless ref($ok_cb) eq 'CODE';

    # Мы на эту очередь и не подписаны, нечего делать
    return unless $queue->{consumed};

    # Если задан callback
    # Выполним заданный $ok_cb, но только если это последняя по счету очередь для отписки
    $ok_cb and do {
        push @{$self->{cbs}->{on_success_cancel}}, sub {
            return if $self->{consumed_count};
            $ok_cb->();
        }, 
    };

    $self->{logger}->log('NOTICE', "cancel: cancelling $queue->{consumer_tag}");

    $self->{rabbitmq}->cancel(
        consumer_tag => $queue->{consumer_tag},
        on_success => sub {},
        on_failure => sub { 
            # Если что-то пошло не так, удалим колбек
            $ok_cb and do { 
                shift @{$self->{cbs}->{on_success_cancel}}; 
            };
            $self->{logger}->log('ERROR', "error occured while cancelling consumption for $queue->{consumer_tag}", @_); 
        }, 
    );

} # cancel_queue

=item $self->consume()

Пройдемся по всем очередям и подпишемся на них, если еще не подписаны (и если надо)
Выставим $self->{conf}->{consume} в 1

=cut
sub consume {
    my ($self, $ok_cb) = @_;

    $self->{conf}->{consume} = 1;

    my $cb;
    if ($ok_cb and ref($ok_cb) eq 'CODE') {
        $cb = sub {
            return unless $self->{consumed_count} == $self->{to_consume}; 
            $ok_cb->();
        }; 
    };

    for my $queue (@{$self->{conf}->{queue}}) {
        $self->consume_queue($queue, $cb);
    }
} # consume

=item $self->on_consume_cb($msg)

Вызывается при приходе сообщения из очереди
Нормализуем сообщение (укорачиваем запись)
Если задан $self->{conf}->{on_consume_cb}, зовем его

=cut
sub on_consume_cb {
    my ($self, $msg) = @_;

    my $message = {
        payload => defined $msg->{body} ? $msg->{body}->payload : '',
        headers => $msg->{header}->{headers},
        delivery_tag => $msg->{deliver}->{method_frame}->{delivery_tag},
    };

    $self->{conf}->{on_consume_cb}->($message);

} # on_consume_cb

=item $self->ack($delivery_tag)

Подтверждение получения сообщения из очереди

=cut
sub ack {
    my ($self, $delivery_tag) = @_;

    $self->{rabbitmq}->ack(delivery_tag => $delivery_tag);
} # ack

1;
