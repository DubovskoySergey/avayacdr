import socket
import pymysql.cursors

it_dep = {'it' : ['1041', '1400', '1401', '1403', '1404', '1405', '1441', '1550', '1551', '1552', '1554', '1558', '3540']}
mag_dep = {'mag' : ['1663', '4157', '4301', '1604', '1697', '1609', '1864', '4129', '4400', '4401', '1736', '1737', '1647', '4127', '4153', '1637',
                    '4152', '4501', '4336', '4526', '4155', '4226', '4303', '1611', '1612', '4229', '4228', '4300' '1751', '1866', '1600', '1635',
                    '1636', '4202', '4156', '4131', '4200', '1615', '1616', '1669', '1671', '1771', '1618', '4126', '4151', '4154', '4101', '4130',
                    '4128', '4160', '1626', '1627', '1632', '1630', '1631', '4337', '4500', '4133', '4502']}
ccall_dep = {'cc' : ['1202', '3517', '1420', '3729', '1083', '3741', '1438' '1492', '1499', '1497', '3754', '1493', '3750', '1543', '3729', '1491', '1492', '1493', '1494',
                    '1495', '1496', '1497', '1498', '1499', '3750', '3751', '3752', '3753', '3754', '3755', '3756', '3757', '3758', '3759', '3760', '3761', '3799']}
wc_dep = {'wc' : ['1297', '1317', '1309', '1308', '1293', '1306', '1305', '1313', '1315', '1316', '1314']}
adm_dep = {'adm' : ['1511', '1034', '1534', '3743', '1075', '1059', '1505', '1432', '1739', '1433', '1506', '1657', '1507', '1658', '1508', '1659', '1509', '1660', '1510', '3543', '1599']}
num_list = (it_dep, mag_dep, ccall_dep, wc_dep, adm_dep)

def find_dep(num):
    for d in num_list:
        for dep, n in d.items():
            for i in n:
                if i == num:

                    return dep

def name_dep(dep):
    if dep == 'it':
        return 'IT отдел'
    elif dep == 'mag':
        return 'Магазины'
    elif dep == 'cc':
        return 'Call Center'
    elif dep == 'wc':
        return 'WorldClass'
    elif dep == 'adm':
        return "Администраторы ГЧ"
    else:
        return 'No depatament'

def write_db(item, *agrs):
    connection = pymysql.connect(host='',
                                 user='',
                                 password='',
                                 db='',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    DBTBL = ""
    DBFLD = ""

    dep_num_call = find_dep(item[3].replace(' ', ''))
    name_dep_call = name_dep(dep_num_call)
    dep_num_dial = find_dep(item[4].replace(' ', ''))
    name_dep_dial = name_dep(dep_num_dial)

    item.append(name_dep_call)
    item.append(name_dep_dial)
    item[1] = item[1] + "00"

    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO "+DBTBL+" ("+DBFLD+") VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, (item))
    except pymysql.InternalError as e:
        print("Error occured: %s"% e)
    else:
        connection.commit()
    finally:
        connection.close()

# Задаем адрес сервера
SERVER_ADDRESS = ('', 5100)

# Настраиваем сокет
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(SERVER_ADDRESS)
server_socket.listen(5)
print('server is running, please, press ctrl+c to stop')

# Слушаем запросы и пишем в db
while True:
    connection, address = server_socket.accept()

    data = connection.recv(1024)
    if not(b'\x00\x00\x00' in data) and not(b'1370' in data):
        str = data.decode("utf-8")
        item=[str[0:6],str[7:11],str[12:17],str[18:33],str[34:57]]
        print(item)
        write_db(item)

    connection.close()
