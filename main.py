import psycopg2


def create_pg_connect(pg_db, pg_user, pg_pass):
    return psycopg2.connect(database=pg_db, user=pg_user, password=pg_pass)


def create_pg_tables(conn):
    with conn:
        with conn.cursor() as cur:

            cur.execute("""
            create table client(
                id serial primary key,
                first_name varchar(100) not null,
                last_name varchar(100) not null,
                constraint uc_name unique (first_name, last_name)
            );
            
            create table client_email(
                client_id int,
                email varchar(100) not null unique,
                constraint fk_client_id foreign key (client_id) references client
            );
            
            create table client_phone(
                client_id int,
                phone_type char(1) not null,
                phone_number varchar(100) not null unique,
                constraint fk_client_id foreign key (client_id) references client,
                constraint cc_phone_type check (phone_type in ('m', 'l'))
            );
            """)

            conn.commit()


class PostgresWork:
    def __init__(self, conn):
        self.conn = conn

    def add_client(self, first_name, last_name):
        with self.conn as conn:
            with conn.cursor() as cur:

                cur.execute("""
                insert into client(first_name, last_name)
                values(%s, %s)
                returning id, first_name, last_name;
                """, (first_name, last_name))

                print(cur.fetchone())

    def add_email(self, client_id, email):
        with self.conn as conn:
            with conn.cursor() as cur:

                cur.execute("""
                insert into client_email(client_id, email)
                values(%s, %s)
                returning client_id, email;
                """, (client_id, email))

                print(cur.fetchone())

    def add_phone(self, client_id, phone_type, phone_number):
        if phone_type not in ['m', 'l']:
            print('Укажите корректный тип телефона, либо "m", либо "l".'
                  '\nm(mobile) - мобильный.'
                  '\nl(landline) - стационарный.')
        else:
            with self.conn as conn:
                with conn.cursor() as cur:

                    cur.execute("""
                    insert into client_phone(client_id, phone_type, phone_number)
                    values(%s, %s, %s)
                    returning client_id, phone_type, phone_number;
                    """, (client_id, phone_type, phone_number))

                    print(cur.fetchone())

    def update_client(
            self, set_first_name=None, set_last_name=None, set_email=None, set_phone_type=None, set_phone_number=None,
            where_client_id=None, where_first_name=None, where_last_name=None, where_email=None,
            where_phone_type=None, where_phone_number=None
    ):
        if not any([set_first_name, set_last_name, set_email, set_phone_type, set_phone_number]):
            print('Вы не передали ни одного значения, на которое нужно изменить')
        elif not any(
            [where_client_id, where_first_name, where_last_name, where_email, where_phone_type, where_phone_number]
        ):
            print('Вы не передали ни одного значения, по которым нужно изменить')
        elif set_phone_type and set_phone_type not in ['m', 'l']:
            print('Передайте в set_phone_type корректный тип телефона, либо "m", либо "l".'
                  '\nm(mobile) - мобильный.'
                  '\nl(landline) - стационарный.')
        elif where_phone_type and where_phone_type not in ['m', 'l']:
            print('Передайте в set_phone_type корректный тип телефона, либо "m", либо "l".'
                  '\nm(mobile) - мобильный.'
                  '\nl(landline) - стационарный.')
        else:
            where_sql = (
                '(select distinct client_id from (select a.id client_id, a.first_name, a.last_name, b.email, '
                'c.phone_type, c.phone_number from client a left join client_email b on b.client_id = a.id '
                'left join client_phone c on c.client_id = a.id) where 1 = 1'
            )
            where_param = []

            if where_client_id:
                where_sql += ' and client_id = %s'
                where_param.append(where_client_id)
            if where_first_name:
                where_sql += ' and first_name = %s'
                where_param.append(where_first_name)
            if where_last_name:
                where_sql += ' and last_name = %s'
                where_param.append(where_last_name)
            if where_email:
                where_sql += ' and email = %s'
                where_param.append(where_email)
            if where_phone_type:
                where_sql += ' and phone_type = %s'
                where_param.append(where_phone_type)
            if where_phone_number:
                where_sql += ' and phone_number = %s'
                where_param.append(where_phone_number)

            if any([set_first_name, set_last_name]):
                sql_client = 'update client set '
                param_client = []
                if set_first_name and set_last_name:
                    sql_client += 'first_name = %s, last_name = %s '
                    param_client.append(set_first_name)
                    param_client.append(set_last_name)
                elif set_first_name and not set_last_name:
                    sql_client += 'first_name = %s '
                    param_client.append(set_first_name)
                elif not set_first_name and set_last_name:
                    sql_client += 'last_name = %s '
                    param_client.append(set_last_name)
                sql_client += f'where id in {where_sql}) returning id, first_name, last_name;'
                param_client.extend(where_param)

                with self.conn as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql_client, tuple(param_client))

                        print(cur.fetchall())

            if set_email:
                sql_email = (
                    f'update client_email set email = %s where client_id in {where_sql}) returning client_id, email;'
                )
                param_email = [set_email]
                param_email.extend(where_param)

                with self.conn as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql_email, tuple(param_email))

                        print(cur.fetchall())

            if any([set_phone_type, set_phone_number]):
                sql_phone = 'update client_phone set '
                param_phone = []
                if set_phone_type and set_phone_number:
                    sql_phone += 'phone_type = %s, phone_number = %s '
                    param_phone.append(set_phone_type)
                    param_phone.append(set_phone_number)
                elif set_phone_type and not set_phone_number:
                    sql_phone += 'phone_type = %s '
                    param_phone.append(set_phone_type)
                elif not set_phone_type and set_phone_number:
                    sql_phone += 'phone_number = %s '
                    param_phone.append(set_phone_number)
                sql_phone += f'where client_id in {where_sql}) returning client_id, phone_type, phone_number;'
                param_phone.extend(where_param)

                with self.conn as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql_phone, tuple(param_phone))

                        print(cur.fetchall())

    def delete_client(self, client_id=None, first_name=None, last_name=None):
        if not any([client_id, first_name, last_name]):
            print('Вы не передали ни одного значения по которому можно удалить клиента')
        else:
            sql = 'delete from client where 1 = 1'
            param = []

            if client_id:
                param.append(client_id)
                sql += ' and id = %s'

            if first_name:
                param.append(first_name)
                sql += ' and first_name = %s'

            if last_name:
                param.append(last_name)
                sql += ' and last_name = %s'

            sql += ' returning id, first_name, last_name;'

            with self.conn as conn:
                with conn.cursor() as cur:

                    cur.execute(sql, tuple(param))

                    print(cur.fetchall())

    def delete_email(
            self, client_id=None, first_name=None, last_name=None, email=None, phone_type=None, phone_number=None
    ):
        if not any([client_id, first_name, last_name, email, phone_type, phone_number]):
            print('Вы не передали ни одного значения по которому можно удалить почту')
        elif phone_type and phone_type not in ['m', 'l']:
            print('Передайте в set_phone_type корректный тип телефона, либо "m", либо "l".'
                  '\nm(mobile) - мобильный.'
                  '\nl(landline) - стационарный.')
        else:
            sql = (
                'delete from client_email where client_id in (select distinct client_id from (select a.id client_id, '
                'a.first_name, a.last_name, b.email, c.phone_type, c.phone_number from client a left join '
                'client_email b on b.client_id = a.id left join client_phone c on c.client_id = a.id) where 1 = 1'
            )
            param = []

            if client_id:
                sql += ' and client_id = %s'
                param.append(client_id)
            if first_name:
                sql += ' and first_name = %s'
                param.append(first_name)
            if last_name:
                sql += ' and last_name = %s'
                param.append(last_name)
            if email:
                sql += ' and email = %s'
                param.append(email)
            if phone_type:
                sql += ' and phone_type = %s'
                param.append(phone_type)
            if phone_number:
                sql += ' and phone_number = %s'
                param.append(phone_number)
            sql += ') returning client_id, email;'

            with self.conn as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(param))

                    print(cur.fetchall())

    def delete_phone(
            self, client_id=None, first_name=None, last_name=None, email=None, phone_type=None, phone_number=None
    ):
        if not any([client_id, first_name, last_name, email, phone_type, phone_number]):
            print('Вы не передали ни одного значения по которому можно удалить телефон')
        elif phone_type and phone_type not in ['m', 'l']:
            print('Передайте в set_phone_type корректный тип телефона, либо "m", либо "l".'
                  '\nm(mobile) - мобильный.'
                  '\nl(landline) - стационарный.')
        else:
            sql = (
                'delete from client_phone where client_id in (select distinct client_id from (select a.id client_id, '
                'a.first_name, a.last_name, b.email, c.phone_type, c.phone_number from client a left join '
                'client_email b on b.client_id = a.id left join client_phone c on c.client_id = a.id) where 1 = 1'
            )
            param = []

            if client_id:
                sql += ' and client_id = %s'
                param.append(client_id)
            if first_name:
                sql += ' and first_name = %s'
                param.append(first_name)
            if last_name:
                sql += ' and last_name = %s'
                param.append(last_name)
            if email:
                sql += ' and email = %s'
                param.append(email)
            if phone_type:
                sql += ' and phone_type = %s'
                param.append(phone_type)
            if phone_number:
                sql += ' and phone_number = %s'
                param.append(phone_number)
            sql += ') returning client_id, phone_type, phone_number;'

            with self.conn as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(param))

                    print(cur.fetchall())

    def find_client(
            self, client_id=None, first_name=None, last_name=None, email=None, phone_type=None, phone_number=None
    ):
        if not any([client_id, first_name, last_name, email, phone_type, phone_number]):
            print('Вы не передали ни одного значения для выборки')
        elif phone_type and phone_type not in ['m', 'l']:
            print('Укажите корректный тип телефона, либо "m", либо "l".'
                  '\nm(mobile) - мобильный.'
                  '\nl(landline) - стационарный.')
        else:
            sql = (
                'select a.id client_id, a.first_name, a.last_name, b.email, c.phone_type, c.phone_number '
                'from client a left join client_email b on b.client_id = a.id '
                'left join client_phone c on c.client_id = a.id where 1 = 1'
            )
            param = []

            if client_id:
                param.append(client_id)
                sql += ' and a.id = %s'
            if first_name:
                param.append(first_name)
                sql += ' and a.first_name = %s'
            if last_name:
                param.append(last_name)
                sql += ' and a.last_name = %s'
            if email:
                param.append(email)
                sql += ' and b.email = %s'
            if phone_type:
                param.append(phone_type)
                sql += ' and c.phone_type = %s'
            if phone_number:
                param.append(phone_number)
                sql += ' and c.phone_number = %s'
            sql += ';'

            with self.conn as conn:
                with conn.cursor() as cur:

                    cur.execute(sql, tuple(param))

                    print(cur.fetchall())


conn1 = create_pg_connect('postgres', 'postgres', 'postgres')
create_pg_tables(conn1)  # закоментировать после одного запуска
dml = PostgresWork(conn1)
# dml.add_client('first_name_1', 'last_name_1')
# dml.add_client('first_name_2', 'last_name_2')
# dml.add_client('first_name_3', 'last_name_3')
# dml.add_email(1, 'email_1')
# dml.add_email(2, 'email_2')
# dml.add_email(3, 'email_3')
# dml.add_phone(1, 'm', '87001111111')
# dml.add_phone(1, 'l', '87176111111')
# dml.add_phone(2, 'm', '87002222222')
# dml.add_phone(2, 'l', '87176222222')
# dml.add_phone(3, 'm', '87003333333')
# dml.add_phone(3, 'l', '87176333333')
# dml.find_client()
# dml.find_client(phone_type='k')
# dml.find_client(client_id=1)
# dml.find_client(first_name='first_name_1')
# dml.find_client(last_name='last_name_1')
# dml.find_client(email='email_1')
# dml.find_client(phone_type='m')
# dml.find_client(phone_number='87001111111')
# dml.delete_phone(first_name='first_name_3', phone_type='l', email='email_3')
# dml.delete_email(first_name='first_name_3', phone_type='l', email='email_3')
# dml.delete_email(first_name='first_name_3', email='email_3')
# dml.delete_client(last_name='last_name_3')
# dml.update_client(
#     set_first_name='jacob', set_last_name='mcneil', where_first_name='first_name_2', where_email='email_2',
#     where_phone_type='l', where_phone_number='87176222222'
# )
