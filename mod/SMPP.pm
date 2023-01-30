package SMPP;

use strict;
use warnings;
#use utf8;
#use open qw(:std :utf8);
#use open ':encoding(utf8)';


=head1 NAME

SMPP

=head1 DESCRIPTION

SMPP daemon

=cut

use Fcntl;
use POSIX qw(strftime);
use Time::HiRes;
use Data::Dumper;
use Encode;
use Conf;
use NetSMPP;
use RabbitMQ;
use Log;

use AnyEvent;
use AnyEvent::DBD::Pg;
use AnyEvent::Socket;

use Storable;

sub new {
    my ($proto, $id, $conf) = @_;

    my $class = ref($proto) || $proto;

    # states:
    # 0 | undef - не приконнекчены
    # 1 - приконнекчены
    # 2 - коннектимся
    # 3 (smsc) - bind_transceiver
    # 4 (smsc) - unbind
    # 10 - ошибка
    my $self  = {
        id => $id,
        conf => $conf,
        state => {
            smsc => 0,
            db => 0,
        },
        ar => undef,

        process_name => 'smpp',

        # Объект RabbitMQ
        rabbitmq => undef,

        # Настройки SMPP
        smpp_cfg => {},

        # exchange RabbitMQ, в который будем выполнять publish
        exchange => {},

        handle => undef,

        # Время последнего enquire_link
        last_enquire_link => 0,

        # Время последнего enquire_link_resp
        last_enquire_link_resp => 0,

        # sequence for enquire_link
        enquire_link_seq => 22,

        # Время отправки bind_transceiver (для отсчета таймаута)
        bind_transceiver_sent => 0,

        # Время (timestamp), которое выставляется в time() + throttle_error_timeout из настроек SMPP 
        # при получении submit_sm_resp  со статусом 88
        throttle_error_timeout => 0,

        # Определение скорости
        # Здесь лежат времена отправленных submit_sm
        sent_queue => [],

        # Сюда кладем сообщения, которые пришли из RabbitMQ, но мы их пока не можем отправить по SMPP
        smpp_msg_queue => [],

        # Время, на которое выставлен таймер отправки SMPP сообщений по скорости
        # Нужно, чтобы не передвигать таймер бесконечно в будущее
        smpp_msg_queue_timer => 0,

        # Всякие параметры
        SMPP_AND => 0b00111100,
        SMPP_DLVR_ST => 0b00100100,
        # Максимальное кол-во октетов в short_message
        SMPP_MAX_SHORT_MESSAGE_LEN => 254,

        # Спул отправленных нами submit_sm, ожидающих submit_sm_resp
        submit_sm_resp_wait => {}, 

        # Данные для hibernating'а
        hibernate_data => {},

        signals => [],
        loops => [],
    };

    bless ($self, $class);

    # включаем протоколирование
    $self->{logger} = Log->new({ log => "$self->{conf}->{log_prefix}_$id.log", debug => $self->{conf}->{debug}, log_prefix => 'SMPP_' . $self->{id}, }) or return;
    $self->{conf}->{dump_prefix} and do {
        my $dump_file = "$self->{conf}->{dump_prefix}_$id.log";
        my $fh;
        open($fh, ">>", $dump_file) or do {
            $self->{logger}->log('ERROR', "new: unable to open dump_prefix log file $dump_file: $!");
        };
        $self->{dumper} = $fh;
    };
    $self->{logger}->log('DEBUG', "new: dumper is", Dumper($self->{dumper}));

######## TEST #########
#    $self->{logger} = Log->new({ log => "$self->{conf}->{log_prefix}_$id.log", debug => $self->{conf}->{debug}, log_prefix => 'SMPP_' . $self->{id}, nofilestdout => 1}) or return;
######## TEST #########

    return unless $self->check_config();

    # пишем пид
    $self->{pidfile} = "$self->{conf}->{pid_prefix}_$id.pid";
    sysopen(PIDFILE, $self->{pidfile}, O_WRONLY | O_EXCL | O_CREAT) or do 
    {
        $self->{logger}->log('ERROR', "Can't create pid file $self->{pidfile}: $!");
        return;
    };
    print PIDFILE $$;
    close PIDFILE;

    # Статусы доставки
    $self->{delivery_status} = {
        1 => 'Enroute', 
        2 => 'Delivered', 
        3 => 'Expired', 
        4 => 'Deleted', 
        5 => 'Not Delivered',
        6 => 'Accepted', 
        7 => 'Unknown', 
        8 => 'Rejected',
        'ENROUTE' => 'Enroute', 
        'DELIVRD' => 'Delivered',
        'EXPIRED' => 'Expired', 
        'DELETED' => 'Deleted',
        'UNDELIV' => 'Not Delivered', 
        'ACCEPTD' => 'Accepted',
        'UNKNOWN' => 'Unknown', 
        'REJECTD' => 'Rejected',
    };

    $self->{hibernate_file} = "$self->{conf}->{hibernate_path}/hibernated_smpp_" . $self->{id};
    # Инициализируем файл, если его нет
    unless (-f $self->{hibernate_file}) {
        eval {
            store {}, $self->{hibernate_file};
        };
        if ($@) {
            $self->{logger}->log('ERROR', "Unable to create $self->{hibernate_file}: $@");
            return;
        }
    }
    $self->load_hibernated_data() or return;

    # Обработка сигналов
    # TERM - заканчиваем работать
    # HUP - ротируем лог

    push @{$self->{signals}}, AnyEvent->signal(
        signal => 'TERM',
        cb => sub {
            $self->{logger}->log('NOTICE', "Got TERM signal, terminating node $self->{id}");
            $self->unbind() if $self->{state}->{smsc} == 1;
            $self->rabbitmq_cancel();
            $self->{run}->send("Terminated");
        },
    );

    push @{$self->{signals}}, AnyEvent->signal(
        signal => 'INT',
        cb => sub {
            $self->{logger}->log('NOTICE', "Got INT signal, terminating node $self->{id}");
            $self->unbind() if $self->{state}->{smsc} == 1;
            $self->rabbitmq_cancel();
            $self->{run}->send("Terminated");
        },
    );

    push @{$self->{signals}}, AnyEvent->signal(
        signal => 'HUP',
        cb => sub {
            $self->{logger}->rotate_log();
        },
    );

    return $self;
} # new

sub process_node {
    my ($self) = @_;

    # Проверка коннекта и реконнект
    push @{$self->{loops}}, AnyEvent->timer(
        after => $self->{conf}->{check_connection_interval},
        interval => $self->{conf}->{check_connection_interval},
        cb => sub { 
            $self->check_connection();
        },
    );

    # Периодические enquire_link'и
    push @{$self->{loops}}, AnyEvent->timer(
        after => $self->{conf}->{enquire_link_interval},
        interval => $self->{conf}->{enquire_link_interval},
        cb => sub { 
            $self->enquire_link();
        },
    );

    # Периодическое сохранение
    push @{$self->{loops}}, AnyEvent->timer(
        after => $self->{conf}->{save_interval},
        interval => $self->{conf}->{save_interval},
        cb => sub {
            $self->save();
        },
    );

    # Чистка спула submit_sm, ожидающих статуса доставки
    push @{$self->{loops}}, AnyEvent->timer(
        after => $self->{conf}->{submit_sm_resp_interval},
        interval => $self->{conf}->{submit_sm_resp_interval},
        cb => sub {
            $self->submit_sm_resp_timeout();
        },
    );

    $self->{logger}->log('NOTICE', "Terminating: ", $self->{run}->recv);

    $self->save();
    $self->finish();

    return;

} # process_node


=item $self->save()

Сохраняем значение last_message_id

=cut
sub save {
    my ($self) = @_;

    return unless $self->{state}->{db} == 1;

    return unless defined $self->{smpp_cfg}->{last_message_id};

    $self->{dbh}->execute(
        'update smpp_config set last_message_id = ? where smpp_config_id = ?', undef, $self->{smpp_cfg}->{last_message_id}, $self->{id},
        sub {
#            my ($rc) = @_;
#            $self->{logger}->log('DEBUG', "save: set last_message_id param for $self->{id} to $self->{smpp_cfg}->{last_message_id} (updated $rc records)");
        },
    );
    return;
} # save

sub finish {
    my ($self) = @_;

############## TEST
    $self->{handle}->destroy();
############## TEST

    $self->hibernate();

    if ($self->{pidfile}) {
        unlink($self->{pidfile}) or do {
            $self->{logger}->log('ERROR', "Can't delete pidfile $self->{pidfile} $!");
        }
    }

} # finish

=item $self->start_node()

Инициализация процесса
Запустим коннекты к БД, кролику и smsc

=cut
sub start_node {
    my ($self) = @_;

    $self->{run} = AnyEvent->condvar;

    $self->{logger}->log('DEBUG', "start_node: starting id $self->{id}");

    # Коннектимся к базе
    # Здесь также попросим connect_to_db в случае успешного коннекта запустить процесс коннекта к smsc и rabbitmq
    $self->connect_to_db(
        sub {
            $self->connect_to_rabbitmq();
            $self->connect_to_smsc();
        },
        sub {},
    );

################### TEST
#    # Читаем STDIN На предмет команд
#    $self->{stdin} = AnyEvent->io(
#        fh => \*STDIN,
#        poll => 'r',
#        cb => sub {
#            $self->parse_command();
#        },
#    );
################### TEST

    return;

} # start_node

=item $self->connect_to_smsc()

Коннект к SMSC

=cut
sub connect_to_smsc {
    my ($self) = @_;


    # Уже приконнекчены или в процессе
    return if $self->{state}->{smsc};

    # Выставляем флаг: мы в процессе коннекта к smsc
    $self->{state}->{smsc} = 2;

    # Вот это либо вызовем сейчас же
    # Либо после загрузки конфигурационных параметров
    my $cb = sub {

        # Есть конфиг из базы - значит, есть имя exchange
        $self->{smsc} = NetSMPP->new(smpp_version => $self->{smpp_cfg}->{version});

################# TEST ####################
#
        tcp_connect $self->{smpp_cfg}->{server} => $self->{smpp_cfg}->{port}, 
            sub { $self->tcp_connect_done_cb(@_); }, 
            sub { $self->tcp_connect_prepare_cb(@_); };

#        $self->{state}->{smsc} = 1;

#
################# TEST ####################
    };

    # Нет конфигурационных параметров
    # Сначала загрузим их, потом будем коннектиться
    unless (keys %{$self->{smpp_cfg}}) {
        $self->load_smpp_config($cb, sub{});
        return;
    }

    # Коннектимся, у нас есть конфиг
    $cb->();
}

=item $self->node_default_config()

Настройки по-умолчанию для ноды
Пока что тут прибито гвоздями, может, вынесем куда-нибудь вместе со всякими SMPP_ADD, delivery status'ами и тд?

=cut
sub node_default_config {
    my ($self) = @_;

    # enquire_mode бывают:
    # recieve - только принимаем, даже если пусто - не шлем
    # send - всегда шлем
    # send_if_silence - сами не начинаем, шлем только если не получаем от той стороны
    my $cfg = {
        active => 0,
        addr_ton => 0,
        addr_npi => 0,
        source_addr_ton => 0,
        source_addr_npi => 0,
        send_rate => 10,
        last_message_id => 0,
        delivery_status_mode => 'regexp',
        delivery_message_field => 'short_message',
        delivery_status_regexp => q{(?:^|\s)id\:(\S+)\s.*\sstat\:(\S+)\s},
        delivery_status_code_ok => 2,
        delivery_message_id_operation => 'hex_to_dec',
        transceiver_connection_timeout => 10,
        bind_transceiver_timeout => 20,
        enquire_mode => 'send_if_silence',
    };

    return $cfg;
} # node_default_config

=item $self->tcp_connect_done_cb($fh, $host, $port)

Сюда попадаем после попытки коннекта к SMSC (удачной или нет)

=cut
sub tcp_connect_done_cb {
    my ($self, $fh, $host, $port) = @_;

############## TEST #################
#    $self->{state}->{smsc} = 1;
#    return;
############## TEST #################

    # Не удалось приконнектиться
    unless ($fh) {
        $self->{logger}->log('ERROR', "connect_to_smsc:", %{$self->{smpp_cfg}}, "unable to connect: $!");
        $self->{state}->{smsc} = 0;
        return;
    }

    # Приконнектились!
    $self->{logger}->log('NOTICE', "connect_to_smsc: connected to server $self->{smpp_cfg}->{server}:$self->{smpp_cfg}->{port} from $self->{smpp_cfg}->{interface}");

    $self->{last_enquire_link} = 0;

    # Для управления дальнейшим обменом по соединению
    $self->{handle} = undef;
    $self->{handle} = new AnyEvent::Handle
        fh => $fh,
        # Это по-умолчанию
        #read_size => 2048,
        on_error => sub {
            my ($hdl, $fatal, $msg) = @_;
            $self->{logger}->log('ERROR', "connect_to_smsc: connection error: $msg fatal: $fatal");
            $hdl->destroy;
            $self->{state}->{smsc} = 0;
            return;
        },
        on_eof => sub {
            my ($hdl) = @_;
            $self->{logger}->log('ERROR', "connect_to_smsc: connection EOF");
            $hdl->destroy;
            $self->{state}->{smsc} = 0;
            return;
        };

    my @buf;
    # В эту функцию будем попадать каждый раз, когда что-нибудь придет из сокета
    @buf = (sub {
        my ($hdl) = @_;
        # Возвращаем 0, если ничего не получили (такое бывает?)
        return 0 unless length($hdl->{rbuf});
        my $data = $hdl->{rbuf};
        #$self->{logger}->log('DEBUG', "got data from smsc socket: ", Dumper($data));

        # Вот тут надо отправить это в NetSMPP для декодирования!
        my $pdu = $self->decode_pdu($data);

        # Декодировали, теперь надо понять, что это вообще такое
        $self->parse_pdu($pdu);

        # Это мы прочитали, очистим буфер
        $hdl->{rbuf} = "";

        # И читаем дальше
        $hdl->push_read(@buf);

        # Буфер очищен
        return 1;
    });

    # Бесконечно читаем из сокета, на каждое прочтение будет вызываться callback из @buf
    $self->{handle}->on_read (sub {
        my ($hdl) = @_;
        $self->{handle}->push_read(@buf);
    });

    # Раз приконнектились, отправим bind_transceiver
    $self->bind_transceiver();
} # tcp_connect_done_cb

=item $self->tcp_connect_prepare_cb($fh)

Это callback-подготовка, тут мы выставляем локальный IP, с которого коннектимся и возвращаем таймаут (обязательно)
Он выполняется ДО реальной попытки установить соединение

=cut
sub tcp_connect_prepare_cb {
    my ($self, $fh) = @_;

    my $bind = AnyEvent::Socket::pack_sockaddr (0, parse_address($self->{smpp_cfg}->{interface}));

    # Не удалось пропарсить адрес
    unless ($bind) {
        $self->{logger}->log('ERROR', "connect_to_smsc: Unable to pack local address $self->{smpp_cfg}->{interface}: $!");
        $self->{run}->send("Unable to pack local address $self->{smpp_cfg}->{interface}");
        return;
    }

    bind ($fh, $bind) or do {
        $self->{logger}->log('ERROR', "connect_to_smsc: Unable to bind local socket to address $self->{smpp_cfg}->{interface}");
        $self->{run}->send("Unable to bind local socket to address $self->{smpp_cfg}->{interface}");
        return;
    };

    $self->{logger}->log('DEBUG', "connect_to_smsc: Successfully bound local socket to $self->{smpp_cfg}->{interface}");

    return $self->{smpp_cfg}->{transceiver_connection_timeout};
}

sub bind_transceiver {
    my ($self) = @_;

    # Это имеет смысл только в стадии коннекта
    return unless $self->{state}->{smsc} == 2;

    # Состояние по SMSC == bind_transceiver
    $self->{state}->{smsc} = 3;

    my @bind_param = ('system_id', $self->{smpp_cfg}->{login}, 'system_type', $self->{smpp_cfg}->{system_type}, 'password', $self->{smpp_cfg}->{password});
    push @bind_param, 'addr_ton', $self->{smpp_cfg}->{addr_ton} if exists($self->{smpp_cfg}->{addr_ton});
    push @bind_param, 'addr_npi', $self->{smpp_cfg}->{addr_npi} if exists($self->{smpp_cfg}->{addr_npi});
    push @bind_param, 'address_range', $self->{smpp_cfg}->{address_range} if exists($self->{smpp_cfg}->{address_range});

    $self->{logger}->log('NOTICE', "bind_transceiver: sending:", @bind_param);

    my $data = $self->{smsc}->bind_transceiver_pdu(@bind_param);
    $self->{bind_transceiver_sent} = time();

    $self->{handle}->push_write($data);

} # bind_transceiver

sub decode_pdu {
    my ($self, $data) = @_;

    my $pdu = $self->{smsc}->decode_pdu($data);

    # NetSMPP нам сюда может напихать ошибок
    my $err = $self->{smsc}->get_smpperror();
    for my $e (@$err) {
        my ($msg, $code) = @$e;
        $self->{logger}->log('ERROR', "decode_pdu: error $code: $msg")
    }

    $self->{smsc}->clear_smpperror();

    return $pdu;
    
} # decode_pdu

=item $self->parse_pdu($pdu)

Парсим полученное из SMSC

=cut
sub parse_pdu {
    my ($self, $pdu) = @_;

    for my $pdu (@$pdu) {
        $self->{logger}->log('NOTICE', "parse_pdu: got pdu:", %$pdu);

        # bind_transceiver_resp
        if ($pdu->{cmd} == 2147483657) {
            $self->got_bind_transceiver_resp($pdu);
            next;
        }

        # enquire_link_resp
        if ($pdu->{cmd} == 2147483669) {
            $self->got_enquire_link_resp($pdu);
            next;
        }

        # enquire_link
        if ($pdu->{cmd} == 21) {
            $self->got_enquire_link($pdu);
            next;
        }

        # unbind
        if ($pdu->{cmd} == 6) {
            $self->{logger}->log('DEBUG', "parse_pdu: got unbind");
            $self->unbind_resp($pdu->{seq});
            next;
        }

        # unbind_resp
        if ($pdu->{cmd} == 2147483654) {
            $self->{logger}->log('DEBUG', "parse_pdu: got unbind_resp");
            if ($self->{state}->{smsc} == 4) {
                $self->{run}->send('Unbind resp');
            }
            return;
        }

        # deliver_sm
        if ($pdu->{cmd} == 5) {
            $self->got_deliver_sm($pdu);
            next;
        }

        # submit_sm_resp
        if ($pdu->{cmd} == 2147483652) {
            $self->got_submit_sm_resp($pdu);
            next;
        }

        # Тут мы не знаем, что это за PDU такое
        $self->{logger}->log('ERROR', "parse_pdu: I do not know what this pdu is:", %$pdu);
    }
} # parse_pdu

=item $self->enquire_link()

Шлем enquire_link

=cut
sub enquire_link {
    my ($self) = @_;

    # Проверим, что у нас не таймаут по enquire_link
    # Это может случиться в состоянии "не шлем никогда сами enquire_link"
    my $time_diff = time()-$self->{last_enquire_link};
    if ($self->{last_enquire_link} and $time_diff > $self->{conf}->{enquire_link_timeout}) {
        $self->{logger}->log('ERROR', "enquire_link: last enquire link was $time_diff seconds ago > timeout value $self->{conf}->{enquire_link_timeout}");
        $self->{state}->{smsc} = 0;
        return;
    }

    # Мы не в рабочей стадии
    return unless $self->{state}->{smsc} == 1;

    # Мы в состоянии "не шлем никогда сами enquire_link"
    return if $self->{smpp_cfg}->{enquire_mode} eq 'receive';

    # Рано еще слать
    return if ($time_diff < $self->{conf}->{enquire_link_interval}); 

    # Шлем enquire_link
    my @param = ('seq', ++$self->{enquire_link_seq});
    $self->{logger}->log('NOTICE', "enquire_link: sending", @param);

    my $data = $self->{smsc}->enquire_link_pdu(@param);

    $self->{last_enquire_link} = time();

    $self->{handle}->push_write($data);
    
} # enquire_link

=item $self->enquire_link_resp()

Шлем enquire_link_resp

=cut
sub enquire_link_resp {
    my ($self, $seq) = @_;

    my @param = ('seq', $seq, 'status', '0x0');
    $self->{logger}->log('NOTICE', "enquire_link_resp: sending", @param);

    my (undef, $data) = $self->{smsc}->enquire_link_resp_pdu(@param);

    $self->{enquire_link_seq} = $seq;
    $self->{last_enquire_link_resp} = time();

    $self->{handle}->push_write($data);

} # enquire_link_resp

sub unbind_resp {
    my ($self, $seq) = @_;

    my @param = ('seq', $seq, 'status', '0x0');
    my (undef, $data) = $self->{smsc}->unbind_resp_pdu(@param);

    #$self->{run}->send('Unbinded');
    $self->{handle}->destroy() if $self->{handle};
    $self->{state}->{smsc} = 0;

} # unbind_resp

=item $self->check_connection()

Проверяем наши соединения
Переконнекчиваемся, если надо

=cut
sub check_connection {
    my ($self) = @_;

    #$self->{logger}->log('DEBUG', "check_connection: states: SMSC $self->{state}->{smsc}, RabbitMQ", defined $self->{rabbitmq} ? $self->{rabbitmq}->{state} : 0, "DB $self->{state}->{db}");

    # Тут мы проверяем таймаут по операции bind_transceiver
    # Если у нас таймаут, мы выставляем состояние по SMSC в undef
    # И таким образом инициализируем переконнект
    if ($self->{state}->{smsc} == 3 and time() - $self->{bind_transceiver_sent} > $self->{smpp_cfg}->{bind_transceiver_timeout}) {
        $self->{logger}->log('ERROR', "check_connection: in_bind_transceiver state", time()-$self->{bind_transceiver_sent}, "seconds, disconnecting");
        $self->{state}->{smsc} = 0;
    }

    # SMSC соединение
    $self->connect_to_smsc() unless $self->{state}->{smsc};

    # DB соединение
    $self->ping_db();

    # RabbitMQ соединение
    #$self->connect_to_rabbitmq() unless defined $self->{rabbitmq} and $self->{rabbitmq}->{state};

} # check_connection

=item $self->ping_db()

Проверяем коннект
Выставляем state в 0, если отвалился
приходится пинговать, тк AnyEvent::DBD::Pg connect() не принимает и не вызывает никаких callback'ов
нам надо как-то определить дисконнект

=cut
sub ping_db {
    my ($self) = @_;

    # Мы не приконнекчены и не в процессе, инициализируем коннект
    # Также, я надеюсь, при дисконнекте от базы по какой-то причине $self->{dbh} должен стать undef
    # Так что мы тут либо отвалились, либо в процессе коннекта
    unless ($self->{dbh} and $self->{state}->{db}) {
        $self->connect_to_db();
        return;
    }

    $self->{dbh}->selectrow_array(
        'select 1', undef, 
        sub {
            my ($rc, @row) = @_;
            #$self->{logger}->log('DEBUG', "ping_db: Got $rc");
            $self->{state}->{db} = $rc ? 1 : 0;
        },
    );
} # ping_db

=item $self->on_consume_cb($qname, $msg)

Вызывается кажды раз, когда что-либо приходит из очереди, на которую мы подписываемся
Это должно быьт исходящее СМС сообщение
Ожидается следующий формат:

$msg = {
    payload = ..., # тут текст (но он может быть пустой или неопределен вообще, например, при биллинге)
    headers => {
        from => ...,
        to => ...,
        smpp_options => {
            8210 => 32,
            ...
        },
        message_id => 12345,
        bulk => 121,
        ...
    }
};

=cut
sub on_consume_cb {
    my $self = shift;
    my $msg = shift;

    $self->{logger}->log('NOTICE', "on_consume_cb: got message: delivery_tag $msg->{delivery_tag}", "headers", %{$msg->{headers}}, "payload", $msg->{payload}, "smpp_options", %{$msg->{headers}->{smpp_options}});
################# TEST
#    $self->{logger}->log('STDOUT', "got message: delivery_tag $msg->{delivery_tag}", "headers", %{$msg->{headers}}, "payload", $msg->{payload}, "\n");
################# TEST

    # Подготовим СМС к отправке
    # В частности, тут мы будем знать точно, сколько реальных смс у нас будет (частей)
    my $message = $self->prepare_sms($msg);    

    # Это в случае, если нам вернулся пустой массив.
    # Сейчас это может быть только если у нас не UDH и сообщение длиннее, чем мы можем отправить
    # Мы его подтвердим и выйдем
    unless (@$message) {
        $self->{rabbitmq}->ack($msg->{delivery_tag});
        return;
    }

    # Сколько мы еще можем отправить?
    my $can_send = int($self->can_send());

    # Отправим столько, сколько можем
    my $sent = 0;
    while ($sent++ < $can_send and my $m = shift(@$message)) {
        $self->submit_sm($m);
    }

    # Все отправилось
    return unless @$message;

    # Остальное мы не можем пока отправить
    # Запоминаем сообщение

    $self->{logger}->log('DEBUG', "on_consume_cb: speed limit: total sent ", scalar @{$self->{sent_queue}}, "first $self->{sent_queue}->[0] last $self->{sent_queue}->[@{$self->{sent_queue}}-1] interval ", $self->{sent_queue}->[@{$self->{sent_queue}}-1] - $self->{sent_queue}->[0], "total in queue", scalar(@$message));
    push @{$self->{smpp_msg_queue}}, @$message;
     
    # Пытаемся запустить таймер на будущее - чтобы обработать запомненные сообщения
    # Запустим отправку отложенных сообщений через x секунд
    $self->set_smpp_msg_queue_timer();

} # on_consume_cb

=item $self->connect_to_rabbitmq()

Приконнектимся к RabbitMQ

=cut
sub connect_to_rabbitmq {
    my ($self) = @_;

    # Уже в процессе коннекта к RabbitMQ или приконнекчены
    return if defined $self->{rabbitmq} and $self->{rabbitmq}->{state};

    # Загрузим конфиг, потом приконнектимся
    $self->{rabbitmq} = RabbitMQ->new('Smpp_' . $self->{id}, { on_consume_cb => sub { $self->on_consume_cb(@_); }, }) or return;

} # connect_to_rabbitmq

sub unbind {
    my ($self) = @_;

############# TEST
#    return
############# TEST
    my @param = ();
    $self->{state}->{smsc} = 4;
    my $data = $self->{smsc}->unbind_pdu();

    $self->{handle}->push_write($data);

} # unbind

sub submit_sm {
    my ($self, $m) = @_;

    my @param;

    $self->{logger}->log('NOTICE', "submit_sm: sending submit_sm:", %$m, %{$m->{smpp_options}}, %{$m->{params}});
################# TEST
#    $self->{logger}->log('STDOUT', "submit_sm:", %$m, %{$m->{smpp_options}}, %{$m->{params}}, "\n");
################# TEST

    # Запомним время, когда мы это отправили, для дальнейшего контроля скорости
    push @{$self->{sent_queue}}, Time::HiRes::time();

    for my $key (keys %{$m->{smpp_options}}) {
        push @param, $key, $m->{smpp_options}->{$key};
    }
    my $data = $self->{smsc}->submit_sm_pdu(@param);

    # Отправим
################### TEST
    $self->{handle}->push_write($data);
################### TEST

    # Время отправки pdu
    $m->{submit_sm_stamp} = time();

    # Отправим ack?
    # Но только если это первая часть
    $self->{logger}->log('DEBUG', "submit_sm: sending ack for message $m->{delivery_tag}", %$m) if $m->{part_idx} == 1 and $m->{delivery_tag};
    $self->{rabbitmq}->ack($m->{delivery_tag}) if $m->{part_idx} == 1 and $m->{delivery_tag};
    $m->{delivery_tag} = 0;

    # Запомним данный submit_sm и будем ждать submit_sm_resp
    $self->{submit_sm_resp_wait}->{$m->{sequence}} = $m;

    # Отправим submit_sm в сервисную очередь
    # Читающий эту очередь сможет в дальнейшем собрать 
    # submit_sm + submit_sm-resp + deliver_sm

    my $route = "service.$self->{id}.submit_sm";
    $self->put_in_queue (
        $route,
        {
            headers => {
                %{$m->{params}},
                submit_sm_stamp => $m->{submit_sm_stamp},
                last_part => $m->{last_part},
                part_idx => $m->{part_idx},
                sequence => $m->{sequence},
            },
        },
    );
} # submit_sm

=item $self->prepare_sms($msg)

$msg - Это то, что пришло из очереди RabbitMQ
В headers могут быть:
flash - для платных невидимых сообщений
data_coding - жестко заданное, иначе берем из конфига, либо модифицируем если flash
ucs2 - жестко заданное, иначе берем из конфига
smpp_options - строка с разделенными ; дополнительными опциональными параметрами
Смотрим в свои настройки на предмет payload: 1 - шлем как есть, если длина сообщения меньше max_payload_len
иначе, а также если 0 - режем на куски длиной max_udh_len или max_udh_ucs2_len
ucs2 Также смотрим в своих настройках, а также из того, что пришло из лоджика

=cut
sub prepare_sms {
    my ($self, $msg) = @_;

    # Это то, что мы вернем в итоге
    # Это будет массив хэшей сообщений
    # В случае payload это будет массив из одного элемента (нерезанный)
    # В случае udh - массив порезанных частей
    my $return = [];

    # SMPP опции, заданные из отправителя
    my $opt = delete $msg->{headers}->{smpp_options} || {};

    # Общие параметры отправки, включая дополнительные опции
    my %options = (
        service_type => $self->{smpp_cfg}->{service_type},
        source_addr_ton => $self->{smpp_cfg}->{source_addr_ton},
        source_addr_npi => $self->{smpp_cfg}->{source_addr_npi},
        source_addr => $msg->{headers}->{source_addr} || $msg->{headers}->{from},
        dest_addr_ton => $self->{smpp_cfg}->{addr_ton},
        dest_addr_npi => $self->{smpp_cfg}->{addr_npi},
        destination_addr => $msg->{headers}->{destination_addr} || $msg->{headers}->{to},
        data_coding => $self->{smpp_cfg}->{data_coding},
        registered_delivery => $self->{smpp_cfg}->{registered_delivery},
        protocol_id => 0,
        short_message => '',
        %$opt,
    );

    # Кодировка
    my $ucs2 = defined($msg->{headers}->{ucs2}) ? delete $msg->{headers}->{ucs2} : $self->{smpp_cfg}->{ucs2};
    
    # Максимальная длина сообщения 
    # По ней смотрим, надо нам использовать UDH или нет
    my $max_len = $ucs2 ? $self->{smpp_cfg}->{max_udh_ucs2_len} : $self->{smpp_cfg}->{max_udh_len};
    $max_len = $self->{smpp_cfg}->{max_payload_len} if $self->{smpp_cfg}->{payload};

    # Размер части сообщения (если мы все таки режем сообщение на куски)
    # 7 и 3 взяты из старого SMPP - там было 160 и длина части 153, 70 и длина части 67
    my $part_len = $ucs2 ? $self->{smpp_cfg}->{max_udh_ucs2_len} - 7 : $self->{smpp_cfg}->{max_udh_len} - 3;

    # flash - выставляется для платных невидимых сообщений
    # Добавляем protocol_id=64 и data_coding=20
    if (delete $msg->{headers}->{flash}) {
        $options{data_coding} = $ucs2 ? 20 : 6;
        $options{protocol_id} = 64;
    }

    my $message = $msg->{payload};
    utf8::decode($message) if $ucs2;

    #$self->{logger}->log('DEBUG', "prepare_sms: Evaluated len values:", join(', ', map {"$_ = $self->{smpp_cfg}->{$_}"} (qw(udh payload max_udh_len max_udh_ucs2_len max_payload_len ucs2))), "evaluated max_len is $max_len, message_len is", length($message));

    # Поправим некоторые значения в параметрах, точнее, запакуем
    for my $p (keys %options) {
        # 16-тиричные значения (это может придти из Лоджика например) ???
        if ($options{$p} and $options{$p} =~ /^0x/) {
            $options{$p} =~ s/0x//g;
            $options{$p} = pack("H*", $options{$p});
        }
    }

    # Порежем сообщение на части
    # Если UDH = true
    # Иначе - ругаемся и ничего не шлем
    if (length($message) > $max_len) {
        unless ($self->{smpp_cfg}->{udh}) {
            $self->{logger}->log('ERROR', "prepare_sms: Message need to be sent with UDH, but UDH disabled:", join(', ', map {"$_ = $self->{smpp_cfg}->{$_}"} (qw(udh payload max_udh_len max_udh_ucs2_len max_payload_len ucs2))), "evaluated max_len is $max_len, message_len is", length($message));
            return [];
        }

        my $counter = int(rand(255));
        my @parts;
        my $tmp = $message;
        while (length($tmp) > $part_len) {
            push @parts, substr($tmp, 0, $part_len);
            $tmp = substr($tmp, $part_len);
        }

        push @parts, $tmp;
        my $np = 1;
        my $prefix = "\x05\x00\x03".chr($counter).chr(scalar(@parts));
        $options{esm_class} = 64;
        foreach my $p (@parts) {
            my $pp = $p;
            if ($ucs2) {
                $pp = encode("UCS-2BE", $p);
            }
            $options{short_message} = $prefix.chr($np).$pp;
            $options{seq} = $self->get_seq_number();

            push @$return, { 
                delivery_tag => $msg->{delivery_tag}, 
                smpp_options => \%options, 
                last_part => $np == scalar @parts ? 1 : 0, 
                part_idx => $np,
                parts_amount => $np, 
                sequence => $options{seq},
                params => $msg->{headers},
            };

            $np++;
       }

    } else {
        if ($ucs2) {
            $message = encode("UCS-2BE", $message);
        } else {
            $message =~ s/(\x0A|\x0D|\t)/ /g;
        }

        use bytes;
        my $field = length($message) > $self->{SMPP_MAX_SHORT_MESSAGE_LEN} ? 'message_payload' : 'short_message';
        $options{$field} = $message;
        $options{seq} = $self->get_seq_number();

        push @$return, { 
            delivery_tag => $msg->{delivery_tag}, 
            smpp_options => \%options, 
            last_part => 1, 
            part_idx => 1,
            parts_amount => 1, 
            sequence => $options{seq},
            params => $msg->{headers},
        };
    }

    return $return;
} # prepare_sms

=item $self->get_seq_number()

Получаем sequence number для submit_sm

=cut
sub get_seq_number {
    my ($self) = @_;

    return ++$self->{smpp_cfg}->{last_message_id}; 

} # get_seq_number


sub parse_delivery_status {
    my ($self, $pdu) = @_;

    my $spec_message_id = $pdu->{receipted_message_id};

    #$self->{logger}->log('DEBUG', "parse_delivery_status: work with pdu $spec_message_id delivery_status_mode $self->{smpp_cfg}->{delivery_status_mode} regexp $self->{smpp_cfg}->{delivery_status_regexp} code_ok $self->{smpp_cfg}->{delivery_status_code_ok} operation $self->{smpp_cfg}->{delivery_message_id_operation}");
    $spec_message_id =~ s/\x00$//;

    my $status;
    my $message = $pdu->{$self->{smpp_cfg}->{delivery_message_field}};

    if ($self->{smpp_cfg}->{delivery_status_mode} eq 'regexp') {
        eval('
            $message =~ /'.$self->{smpp_cfg}->{delivery_status_regexp}.'/; 
            if (!length($spec_message_id)) { 
                $spec_message_id = $1; 
                if ($self->{smpp_cfg}->{delivery_message_id_operation} eq "hex_to_dec") {
                    $spec_message_id = hex($spec_message_id); 
                } elsif ($self->{smpp_cfg}->{delivery_message_id_operation} eq "dec_to_hex") {
                    $spec_message_id = sprintf("%x", $spec_message_id);
                }
            } 
            $status = $2;
        ');
        if ($@) {
            $status = undef,
        }

    } elsif ($self->{smpp_cfg}->{delivery_status_mode} eq 'binary_field') {
        $status = ord($message);

    } elsif ($self->{smpp_cfg}->{delivery_status_mode} eq 'binary_field_case') {
        $status = ord($message);
        unless (exists($self->{delivery_status}->{$status})) {
            $status = undef;
        }
    }

    $self->{logger}->log('NOTICE', "parse_delivery_status: delivery_status_mode $self->{smpp_cfg}->{delivery_status_mode} regexp $self->{smpp_cfg}->{delivery_status_regexp} code_ok $self->{smpp_cfg}->{delivery_status_code_ok} operation $self->{smpp_cfg}->{delivery_message_id_operation} spec_message_id $spec_message_id status $status");

    return ($spec_message_id, $status);

} # parse_delivery_status

sub got_delivery_status {
    my ($self, $pdu) = @_;

    # В любом случае ответим deliver_sm_resp ОК (status = 0)
    $self->deliver_sm_resp('seq', $pdu->{seq}, 'status', '0x00', 'message_id', '');
    
    # Теперь распарсим, что мы такое получили
    my ($message_id, $status) = $self->parse_delivery_status($pdu);

    # Не распарсилось
    unless (defined($status)) {
        $self->{logger}->log('ERROR', "got_delivery_status: unable to parse delivery status for pdu", %$pdu);
        return;
    }

    # Отправим статус отправки в лоджик
    $self->put_in_queue (
        "service.$self->{id}.deliver_sm",
        {
            headers => {
                pdu_message_id => $message_id,
                event_type => undef,
                status => $status eq $self->{smpp_cfg}->{delivery_status_code_ok} ? 1 : 0,
                pdu_status => $status, 
                status_text => $self->{delivery_status}->{$status} || undef,
                deliver_sm_stamp => time(),
                to => $pdu->{source_addr},
                from => $pdu->{destination_addr},
            }, 
        },
    );

} # got_delivery_status

sub deliver_sm_resp {
    my $self = shift;
    my @param = @_;

    $self->{logger}->log('DEBUG', "deliver_sm_resp: sending", @param);
    my (undef, $data) = $self->{smsc}->deliver_sm_resp_pdu(@param);

    $self->{handle}->push_write($data);
} # deliver_sm_resp

# Получили bind_transceiver_resp
sub got_bind_transceiver_resp {
    my ($self, $pdu) = @_;

    $self->{logger}->log('NOTICE', "got_bind_transceiver_resp", %$pdu);

    if ($self->{state}->{smsc} == 3) {
        if ($pdu->{status} == 0) {
            $self->{logger}->log('DEBUG', "got_bind_transceiver_resp: bind_transceiver operation OK");
            # Приконнектились, и можем работать с SMSC
            $self->{state}->{smsc} = 1;

            # Сразу отправим (или попытаемся) сообщения, которые пришли из кролика, пока мы коннектились
            $self->send_smpp_msg_queue();

        } else {
            $self->{logger}->log('ERROR', "got_bind_transceiver_resp: bind_transceiver error");
            $self->{state}->{smsc} = 0;
        } 
    }
    return;
} # got_bind_transceiver_resp

=item $self->got_enquire_link_status($pdu)

Получили ответ на наш enquire_link (enquire_link_resp)

=cut
sub got_enquire_link_resp {
    my ($self, $pdu) = @_;
    $self->{logger}->log('DEBUG', "got_enquire_link_resp", %$pdu);
    $self->{last_enquire_link_resp} = time();

    my $error;
    # Проверим, что это ответ на отправленный нами enquire_link
    $error = "enquire_link sequence $pdu->{seq} is not equal to sent $self->{enquire_link_seq}" unless $pdu->{seq} == $self->{enquire_link_seq};
    # Статус должен быть 0
    $error = "got error status $pdu->{status} for enquire_link_resp" unless $pdu->{status} == 0;

    if ($error) {
        $self->{logger}->log('ERROR', $error);
        # Переконнектимся
        $self->{state}->{smsc} = 0;
    }

    return;
} # got_enquire_link_resp

=item $self->got_enquire_link($pdu)

Получили enquire_link - отвечаем

=cut
sub got_enquire_link {
    my ($self, $pdu) = @_;
    $self->{logger}->log('DEBUG', "parse_pdu: got enquire_link", %$pdu);
    $self->{last_enquire_link} = time();

    # Надо ответить
    $self->enquire_link_resp($pdu->{seq});
    return;
}

=item $self->parse_deliver_sm($pdu)

Получили deliver_sm
Разберемся, что это - статус доставки или пользовательские входящее
И обработаем

=cut
sub got_deliver_sm {
    my ($self, $pdu) = @_;
    $self->{logger}->log('DEBUG', "got_deliver_sm", %$pdu);

    $self->{logger}->log('DEBUG', "got_deliver_sm: dumper is", Dumper($self->{dumper}));
    $self->{dumper} and do {
        my $fh = $self->{dumper};
        print $fh Dumper($pdu), "\n";
    };

    # Определим, что это за deliver_sm
    # Может быть запрос от абонента либо статус доставки
    my $esm_class = $pdu->{esm_class} & $self->{SMPP_AND};

    # Статус доставки
    if ($esm_class & $self->{SMPP_DLVR_ST}) {
        $self->got_delivery_status($pdu);
    # Пользовательское входящее сообщение
    } elsif ($esm_class != 4) {
        $self->incoming_message($pdu);
    } else {
        $self->{logger}->log('ERROR', "got_deliver_sm: I do not know what this message is");
    }
    return;
} # got_deliver_sm

=item $self->incoming_message($pdu)

Пользовательское входящее сообщение

=cut
sub incoming_message {
    my ($self, $pdu) = @_;

    $self->{logger}->log('DEBUG', "incoming_message", %$pdu);

    # Если вдруг нет source_addr ???
    unless ($pdu->{source_addr}) {
        $self->{logger}->log('ERROR', "incoming_message: no source_addr found in pdu");
        $self->deliver_sm_resp('seq', $pdu->{seq}, 'status', '0x557', 'message_id', '');
        return;
    }

    my $message;
    if (exists($pdu->{message_payload}) and length($pdu->{message_payload})) {
        $message = delete $pdu->{message_payload};
    } else {
        $message = delete $pdu->{short_message};
    }

    #################
    #
    # ТЕПЕРЬ ТУТ НАДО РАСПАРСИТЬ СООБЩЕНИЕ ОБРАТНО
    # или пусть logick этим занимается?
    #
    #################
    $pdu->{data_coding} == 8 and do {
        $message = decode('UCS-2BE', $message);
        utf8::encode($message);
        $self->{logger}->log('DEBUG', "message AFTER ", Dumper($message));
    };

    my $smpp_options;
    %$smpp_options = map { $_ => Net::AMQP::Value::String->new($pdu->{$_}) } keys %$pdu;
    defined $pdu->{source_port} and do {
        $smpp_options->{source_port} = unpack 'nnn', $pdu->{source_port};
        $self->{logger}->log('NOTICE', "incoming_message: found source_port parameter $pdu->{source_port}, unpacked to $smpp_options->{source_port}");
    };

    my $msg = {
        payload => $message,
        headers => {
            from => delete $pdu->{source_addr},
            to => delete $pdu->{destination_addr},
            event_type => 'smpp.' . $self->{id},
            smpp_options => $smpp_options,
        },
    };

    # Отправим сообщение в лоджик через rabbitmq
    $self->put_in_queue(
        'smpp.'.$self->{id},
        $msg,
        {
            on_ack_cb => sub {
                $self->deliver_sm_resp('seq', $pdu->{seq}, 'status', '0x00', 'message_id', '');
            },
            on_nack_cb => sub {
                $self->deliver_sm_resp('seq', $pdu->{seq}, 'status', '0x557', 'message_id', '');
            },
        },
    );
    
} # incoming_message

=item $self->got_submit_sm_resp($pdu)

Получили submit_sm_resp

=cut
sub got_submit_sm_resp {
    my ($self, $pdu) = @_;
    $self->{logger}->log('DEBUG', "got_submit_sm_resp", %$pdu);

    # Есть ли у нас такой submit_sm в submit_sm_resp_wait?
    # Удалим из спула ожидания в любом случае
    my $submit_sm = delete $self->{submit_sm_resp_wait}->{$pdu->{seq}} || {};
    $submit_sm->{params} ||= {};

    # Если нам заданы статусы, по которым мы должны ретраить, проверим статус
    # 0 не ретраим в любом случае
    %$submit_sm and $pdu->{status} != 0 and do {
        for my $rs (@{$self->{smpp_cfg}->{submit_sm_retry_status}}) {
            #next unless hex($pdu->{status}) == $rs;
            next unless $pdu->{status} == $rs;

            # Retry - кладем сообщение в начало очереди
            $self->{logger}->log('NOTICE', "got_submit_sm_resp: sequence $pdu->{seq} message_id $submit_sm->{params}->{message_id} status $pdu->{status} retrying");
            $submit_sm->{sequence} = $self->get_seq_number();
            $submit_sm->{smpp_options}->{seq} = $submit_sm->{sequence};
            unshift @{$self->{smpp_msg_queue}}, $submit_sm;

            # Пытаемся запустить таймер на будущее - чтобы обработать запомненные сообщения
            # Запустим отправку отложенных сообщений через x секунд
            $self->set_smpp_msg_queue_timer();
            last;
        }
    };

    # статус 88 - throttle error
    #if (hex($pdu->{status}) == 88 and $self->{smpp_cfg}->{throttle_error_timeout}) {
    if ($pdu->{status} == 88 and $self->{smpp_cfg}->{throttle_error_timeout}) {
        $self->{throttle_error_timeout} = time() + $self->{smpp_cfg}->{throttle_error_timeout};
        #$self->{logger}->log('ERROR', "got_submit_sm_resp: throttle error (", hex($pdu->{status}), ") pause $self->{smpp_cfg}->{throttle_error_timeout} (up to $self->{throttle_error_timeout})");
        $self->{logger}->log('ERROR', "got_submit_sm_resp: throttle error (", $pdu->{status}, ") pause $self->{smpp_cfg}->{throttle_error_timeout} (up to $self->{throttle_error_timeout})");
        return;
    }

    # Отправим статус submit_sm_resp 
    my $route = "service.$self->{id}.submit_sm_resp";
    $self->put_in_queue(
        $route,
        {
            headers => {
                %{$submit_sm->{params}},
                sequence => $pdu->{seq}, 
                event_type => undef,
                #status => hex($pdu->{status}) == 0 ? 1 : 0,
                status => $pdu->{status} == 0 ? 1 : 0,
                #pdu_status => hex($pdu->{status}), 
                pdu_status => $pdu->{status}, 
                pdu_message_id => $pdu->{message_id},
                submit_sm_resp_stamp => time(),
                submit_sm_stamp => $submit_sm->{submit_sm_stamp},
                last_part => $submit_sm->{last_part},
                part_idx => $submit_sm->{part_idx},                
            }, 
        }, 
    );

    return;
} # got_submit_sm_resp

sub can_send {
    my ($self) = @_;

    my $ctime = Time::HiRes::time();

    # Вернем 0 , если мы не приконнектились к SMSC
    return 0 unless $self->{state}->{smsc} == 1;

    # Мы в паузе по throttle error (статус 88)
    return 0 if $self->{throttle_error_timeout} > time();

    # Выгребаем из $self->{sent_queue} (из начала массива) те значения, для которых секунда уже прошла
    while (my $t = shift @{$self->{sent_queue}}) {
        if ($t + $self->{smpp_cfg}->{send_rate_period} > $ctime) {
            unshift @{$self->{sent_queue}}, $t;
            last;
        }
    }

    # Проверяем скорость
    if (@{$self->{sent_queue}}) {
        return ($ctime - $self->{sent_queue}->[0]) * $self->{smpp_cfg}->{send_rate} - scalar @{$self->{sent_queue}};
    }

    return $self->{smpp_cfg}->{send_rate};
} # can_send

sub min {
    my ($self, $t1, $t2) = @_;

    return $t2 if $t1 > $t2;
    return $t1;
}

=item $self->send_smpp_msg_queue()

Отправляем сообщения, отложенные по скорости или еще по какой-то причине (SMPP)

=cut
sub send_smpp_msg_queue {
    my ($self) = @_;

    my $ctime = Time::HiRes::time();
    my $can_send = int($self->can_send());

    #$self->{logger}->log('DEBUG', "send_smpp_msg_queue: started can_send $can_send, msg_queue", scalar @{$self->{smpp_msg_queue}});

    my $sent = 0;
    while ($sent++ < $can_send and my $msg = shift @{$self->{smpp_msg_queue}}) {

        $self->{logger}->log('DEBUG', "send_smpp_msg_queue: sending message", %{$msg->{headers}}, %{$msg->{headers}->{smpp_options}}, %{$msg->{headers}->{params}});
        $self->submit_sm($msg);
    }

    # Если в очереди еще остались сообщения, запускаем следующий таймер
    if (@{$self->{smpp_msg_queue}}) {
        $self->set_smpp_msg_queue_timer();
    }

} # send_smpp_msg_queue


sub set_smpp_msg_queue_timer {
    my ($self) = @_;

    # Если мы не в рабочем состоянии по SMSC - ничего не делаем (после коннекта запустится отправка отложенных сообщений)
    return unless $self->{state}->{smsc} == 1;

    my $ctime = Time::HiRes::time();
    my $tm = $self->min($self->{sent_queue}->[0] + $self->{smpp_cfg}->{send_rate_period}, $self->{sent_queue}->[0] + (scalar @{$self->{sent_queue}} + 1)/$self->{smpp_cfg}->{send_rate}) - $ctime;

    # Если предыдущее значение таймера в будущем - ничего не делаем
    return if $self->{smpp_msg_queue_timer} > $ctime+$tm;

    $self->{smpp_msg_queue_timer} = $ctime+$tm;

    $self->{logger}->log('DEBUG', "set_smpp_msg_queue_timer: starting send timer to $self->{smpp_msg_queue_timer} value (after $tm)");

    # Запустим отправку отложенных сообщений через x секунд
    $self->{send_timer} = AnyEvent->timer(
        after => $tm,
        cb => sub { 
            $self->send_smpp_msg_queue();
        },
    );

} # set_smpp_msg_queue_timer

sub connect_to_db {
    my ($self, $ok_cb, $err_cb) = @_;

    # Это выполним, если не приконнектились
    $err_cb ||= sub {};

    $ok_cb ||= sub {};

    # Уже приконнектились или в процессе
    return if $self->{state}->{db};

    $self->{logger}->log('DEBUG', "connect_to_db: connecting to DB");

    # Флаг, означающий, что мы в процессе коннекта
    $self->{state}->{db} = 2;

    # Коннект к базе
    $self->{dbh} = AnyEvent::DBD::Pg->new(
        $self->{conf}->{db}->[0], undef, undef,
        { pg_enable_utf8 => 1, },
    );

    # Не приконнектились
    # Попробуем в следующий раз
    unless ($self->{dbh}->connect()) {
        $self->{logger}->log('ERROR', "connect_to_db: DB connection error");
        $self->{state}->{db} = 0;

        # Запустим то, что нас попросили (if any)
        $err_cb->();
        return;
    }

    # Приконнектились !

    # При переполнении вот этого запросы наши просто не выполняются
    #############################
    #
    # ПОФИКСИТЬ???
    #
    #############################
    $self->{dbh}->queue_size(1000000);

    $self->{logger}->log('NOTICE', "connect_to_db: DB connection successfully established");
    $self->{state}->{db} = 1;

    # Выполним то, что нас попросили (if any)
    $ok_cb->();

} # connect_to_db

=item $self->load_smpp_config($ok_cb, $err_cb)

Загружаем параметры SMPP

=cut
sub load_smpp_config {
    my ($self, $ok_cb, $err_cb) = @_;

    $ok_cb ||= sub {};
    $err_cb ||= sub {};

    # Нет коннекта к базе
    return unless $self->{state}->{db} == 1;

    # Получим наши параметры
    my @params = (qw(
        name active server port login password system_type 
        addr_ton addr_npi source_addr_ton source_addr_npi 
        send_rate last_message_id interface payload udh
        delivery_status_mode delivery_message_field delivery_status_regexp
        delivery_status_code_ok delivery_message_id_operation
        data_coding ucs2 max_udh_len max_udh_ucs2_len max_payload_len
        registered_delivery service_type send_rate_period version
        submit_sm_resp_timeout submit_sm_retry_status throttle_error_timeout
    ));

    # Присвоим дефолтные значения тем параметрам, которые не заданы в smpp_config
    $self->{smpp_cfg} = $self->node_default_config();

    # Выбираем наши параметры

    $self->{dbh}->selectrow_array(
        'select ' . join(',', @params) . ' from smpp_config where smpp_config_id = ? and active', undef, $self->{id}, 
        sub {
            my ($rc, @row) = @_;

            # Нет активного конифга для такого smpp_config_id!
            unless ($rc) {
                $self->{logger}->log('ERROR', "load_smpp_config: no active node $self->{id} found in database!");

                # Позовем callback, если нас просили
                $err_cb->();

                # Нечего больше делать
                $self->{run}->send("No active node $self->{id} found");

                return;
            }

            # Нашелся активный конфиг
            @{$self->{smpp_cfg}}{@params} = @row;

            # sequence number for submit_sm
            $self->{smpp_cfg}->{last_message_id} ++;

            $self->{logger}->log('NOTICE', "load_smpp_config: loaded configuration params for node $self->{id}:", %{$self->{smpp_cfg}});

            # Позовем то, что нас попросили при вызове (if any)
            $ok_cb->();

        },
    );

} # load_smpp_config

sub submit_sm_resp_timeout {
    my ($self) = @_;

    return unless defined $self->{smpp_cfg}->{submit_sm_resp_timeout};

    for my $seq (sort keys %{$self->{submit_sm_resp_wait}}) {
        # Эти еще подождут
        last if $self->{submit_sm_resp_wait}->{$seq}->{submit_sm_stamp} + $self->{smpp_cfg}->{submit_sm_resp_timeout} > time();          

        # У этих истек срок ожидания респа
        $self->{logger}->log('ERROR', "submit_sm_resp_timeout: timed out submit_sm sequence $seq message_id $self->{submit_sm_resp_wait}->{$seq}->{params}->{message_id}");

        # Больше не ждем
        my $m = delete $self->{submit_sm_resp_wait}->{$seq};

        $self->put_in_queue (
            "service.$self->{id}.submit_sm_resp",
            {
                headers => {
                    %{$m->{params}},
                    event_type => undef,
                    submit_sm_stamp => $m->{submit_sm_stamp},
                    last_part => $m->{last_part},
                    part_idx => $m->{part_idx},
                    sequence => $m->{sequence},
                    submit_sm_resp_stamp => undef,
                    status => 0,
                    pdu_status => undef,
                    status_text => 'No submit_sm_resp',
                    pdu_message_id => undef,
                },
            },
        );     
    }
    
} # submit_sm_resp_timeout

=item $self->put_in_queue($route, $msg, $args)

Кладем сообщение в очередь
По сути просто зовем put_in_queue из RabbitMQ модуля
В необязательных $args могут быть:
on_ack_cb => sub {}
on_nack_cb => sub {}
expiration
priority

=cut
sub put_in_queue {
    my ($self, $route, $msg, $args) = @_;

    # Иначе нам не распознать в читателе служебной очереди
    $msg->{headers}->{smpp_config_id} = $self->{id};

    # Шлем сообщение в очередь
    $self->{rabbitmq}->put_in_queue($route, $msg, $args);
} # put_in_queue

sub hibernate {
    my ($self) = @_;

    $self->{logger}->log('NOTICE', "hibernate start $self->{hibernate_file}");

    $self->{hibernated_data} = {};

    for my $key (qw(sent_queue smpp_msg_queue submit_sm_resp_wait)) {
        $self->{hibernated_data}->{$key} = $self->{$key};
    };

    eval {
        store $self->{hibernated_data}, $self->{hibernate_file};
    };
    if ($@) {
        $self->{logger}->log('ERROR', "hibernate: unable to store data: $@");
    }
    
    $self->{logger}->log('DEBUG', "hibernate: saving", Dumper($self->{hibernated_data}));

} # hibernate

sub load_hibernated_data {
    my ($self) = @_;

    $self->{logger}->log('NOTICE', "load_hibernated_data start $self->{hibernate_file}");

    eval {
        $self->{hibernated_data} = retrieve $self->{hibernate_file};
    };
    if ($@) {
        $self->{logger}->log('ERROR', "load_hibernated_data: unable to retrieve from $self->{hibernate_file}: $@");
        return;
    }

    # Чистим данные в файле
    eval {
        store {}, $self->{hibernate_file};
    };
    if ($@) {
        $self->{logger}->log('ERROR', "load_hibernated_data: unable to store to $self->{hibernate_file}: $@");
        return;
    }

    $self->{logger}->log('DEBUG', "load_hibernated_data:", Dumper($self->{hibernated_data}));

    for my $key (qw(sent_queue smpp_msg_queue submit_sm_resp_wait)) {
        next unless exists $self->{hibernated_data}->{$key};
        $self->{logger}->log('DEBUG', "load_hibernated_data: loaded key $key", Dumper($self->{hibernated_data}->{$key}));
        $self->{$key} = $self->{hibernated_data}->{$key};
    }

    return 1;
    
} # load_hibernated_data

sub check_config {
    my ($self) = @_;

    for my $param (qw(
        log_prefix pid_prefix enquire_link_interval enquire_link_timeout 
        check_connection_interval save_interval hibernate_path db
        submit_sm_resp_interval
        )
    ) 
    {
        unless (defined $self->{conf}->{$param}) {
            $self->{logger}->log('ERROR', "check_config: parameter $param is not set in Conf!");
            return;
        }
        if (ref($self->{conf}->{$param}) eq 'ARRAY' and !@{$self->{conf}->{$param}}) {
            $self->{logger}->log('ERROR', "check_config: parameter $param is empty in Conf!");
            return;
        }
    }

    return 1;
    
} # check_config

sub rabbitmq_cancel {
    my ($self) = @_;

    $self->{rabbitmq}->cancel();

} # rabbitmq_cancel

#################### TEST
sub parse_command {
    my ($self) = @_;
    chomp (my $input = <STDIN>);

    my @data = split(' ', $input);
    my $command = shift(@data);

    return unless defined $command;

    # Помощь
    if ($command eq 'help') {
        return $self->help();
    }

    # Выход
    if ($command eq 'quit' or $command eq 'exit') {
        $self->{run}->send("Exiting");
        return;
    }

    # submit_sm_resp
    # ищем следующие параметры:
    # sequence
    # smsc_message_id
    # status
    # Это если собирать статусы
    if ($command eq 2 or $command eq 'resp' or $command eq 'submit_sm_resp') {
        my ($seq, $status, $mid) = @data;
        $seq and defined $status or do {
            $self->{logger}->log('STDOUT', "not enough parameters for submit_sm_resp command\n");
            return $self->help();
        };
        $self->got_submit_sm_resp({seq => $seq, message_id => $mid, status => sprintf('%x', $status)});
        return;
    }

    # deliver_sm (статус)
    if ($command eq 4 or $command eq 'status' or $command eq 'deliver_sm_status') {
        my ($message_id, $status) = @data;
        $message_id and defined $status or do {
            $self->{logger}->log('STDOUT', "not enough parameters for deliver_sm command\n");
            return $self->help();
        };
        # Отправим статус отправки в лоджик
        $self->put_in_queue (
            "service.$self->{id}.deliver_sm",
            {
                headers => {
                    pdu_message_id => $message_id,
                    event_type => undef,
                    status => $status eq $self->{smpp_cfg}->{delivery_status_code_ok} ? 1 : 0,
                    pdu_status => $status, 
                    status_text => $self->{delivery_status}->{$status} || undef,
                    deliver_sm_stamp => time(),
                }, 
            },
        );
        return;
    }

    # deliver_sm (входящее сообщение)
    if ($command eq 5 or $command eq 'inc' or $command eq 'incoming_sms') {
        my ($from, $to, @msg) = @data;
        $from and $to or do {
            $self->{logger}->log('STDOUT', "not enough parameters for incoming_sms command (from, to)\n");
            return $self->help();
        };
        $self->put_in_queue(
            'smpp.' . $self->{id}, 
            {
                payload => join(' ', @msg),
                headers => {
                    event_type => "smpp.$self->{id}",
                    from => $from,
                    to => $to,
                    smpp_options => {},
                },
            },        
        );
        return;
    }

    # Тест cancel()
    if ($command eq 'cancel') {
        $self->{rabbitmq}->cancel(sub {$self->{logger}->log('STDOUT', "Successfully cancelled all consumptions");});
        return;
    }

    # Тест consume()
    if ($command eq 'consume') {
        $self->{rabbitmq}->consume(sub {$self->{logger}->log('STDOUT', "Successfully consumed to queues");});
        return;
    }

    # Неизвестная команда
    $self->help();
} # parse_command

sub help {
    my ($self) = @_;

    $self->{logger}->log("STDOUT", "Usage: $0 <command> [arg1, arg2, ... argX]"); 
    $self->{logger}->log("STDOUT", "Commands:");
    $self->{logger}->log("STDOUT", "2|resp|submit_sm_resp <sequence> <status> <smsc_message_id> - submit_sm_resp");
    $self->{logger}->log("STDOUT", "4|status|deliver_sm_status <message_id> <status> - delivery status");
    $self->{logger}->log("STDOUT", "5|inc|incoming_sms <from> <to> <message> - incoming sms");
    $self->{logger}->log("STDOUT", "help - this message");
    $self->{logger}->log("STDOUT", "quit|exit - exit from script");
    $self->{logger}->log("STDOUT", "cancel - close all consumptions");
    $self->{logger}->log("STDOUT", "consume - consume to queues\n");
} # help
#################### TEST


1;
