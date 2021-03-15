import mcon
import time
import mysql.connector
from bs4 import BeautifulSoup as bs
from random import choice
import requests

data_all_games = []
dict_gener_id = []
all_games_screens = []
MINTASKQUEUE = 300


def connect():
    """ Connect to MySQL database """
    try:
        global conn
        conn = mysql.connector.connect(host=mcon.host,
                                       database=mcon.database,
                                       user=mcon.user,
                                       password=mcon.password)
        if conn.is_connected():
            print_log('connected to MySQL database')
        return conn

    except Exception as e:
       print('%s - can`t connect, Error:%s' % (time.strftime("%H:%M:%S"), e))





def load_genre_id():
    cursor = conn.cursor()
    lis = []
    cursor.execute("SELECT `id`,`app_genre_name` FROM `genre_id`")
    for c in cursor:
        sp = (c[1],c[0])
        lis.append(sp)
    return dict(lis)

def load_data():
    global dict_gener_id
    dict_gener_id = load_genre_id()


def doing_new_tasks():
    global data_all_games
    load_all_links_from_bd(conn)
    for task in load_task_from_base(conn):
        load_last_links_from_bd(conn)
        ts = Task()
        '''
        задачи на поиск новых URL 
        '''
        if task[2] == 3:  # задачи на поиск новых урл на сайте
            print_log('начали делать новый таск по сбору линков на странице')
            ts.add_task(task[0], task[1], task[2])
            try:
                if ts.grab_page(): ts.sort_and_save_grab_urls()
            except Exception as e:
                print_log('не получилось загрузить %s, ошибка статус %s ' % (task[1], e))
                set_task_status(task[0], 0, task[1], e)
                break

        '''
           задачи на парсинг страницы с игрой 
        '''

        if task[2] == 1:
            print_log('начали делать задачу с парсингом игры')
            n_app = NewApp()
            try:
                if n_app.grab_app_page(task[1]):
                    n_app.app_page_parse(task[0], task[1])
                else:
                    set_task_status(task[0], 0, task[1], 'не смогли грабнуть страницу')

            except Exception as e:
                print_log('не получилось загрузить URL = %s, ошибка статус %s' % (task[1], str(e)))
                set_task_status(task[0], 0, task[1], str(e))

            if len(data_all_games)>20:
                save_all_apps_data_to_base(data_all_games)

            if len(all_games_screens) > 50:
                save_apps_screens_to_bd(all_games_screens)


def random_proxy ():
    proxy = open('proxy.txt').read().split('\n')
    return choice(proxy)

def random_ua ():
    ua = open('ua.txt').read().split('\n')
    return choice(ua)

def print_log(str = ''):
    print('%s - %s' % (time.strftime("%H:%M:%S"), str))
    f = open('logs/'+time.strftime("%d%m%y")+'_log.txt', 'a')
    f.write((time.strftime("%H:%M:%S") +' ' + str + '\n'))
    f.close()

def load_task_from_base(conn):
    all_tasks = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks where active = %s", [1])
    for c in cursor:
        all_tasks.append(c)
    print_log(f'загрузили {len(all_tasks)} активных тасков')
    return all_tasks

def load_all_links_from_bd(conn):
    print_log('загружаем все ссылки с базы данных')
    global all_links_from_base
    all_links_from_base = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_site_urls ")
    k=0
    for c in cursor:
        if c[2] != k:
            all_links_from_base.insert(0, (c[1], c[2]))
            k = c[2]
        else:
            all_links_from_base.append((c[1], c[2]))
    print_log('закончили загружать URL с BD всего загрузили '+str(len(all_links_from_base))+' урлов')
    return True

def load_last_links_from_bd(conn):
    print_log(f'обновляем последние данные из базы в переменную all_links_from_base')
    global all_links_from_base
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM `all_site_urls` ORDER BY `id` DESC LIMIT 400")
    k=0
    for c in cursor:
        if c[2] != k:
            all_links_from_base.insert(0, (c[1], c[2]))
            k = c[2]
        else:
            all_links_from_base.append((c[1], c[2]))
    #exit()
    print_log('закончили загружать URL с BD всего загрузили '+str(len(all_links_from_base))+' урлов')
    return True


def load_all_links_from_bd_for_tasks(conn):
    print_log('загружаем все ссылки с базы данных для формирования тасков')
    global all_links_from_base_for_task
    all_links_from_base_for_task = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_site_urls where (url_link NOT IN(SELECT `task_url` FROM `tasks`)) AND (`url_link_type_id` in (1)) LIMIT 1000")
    k = 0
    for c in cursor:

        if c[2] != k:
            all_links_from_base_for_task.insert(0, (c[1], c[2]))
            k = c[2]
        else:
            all_links_from_base_for_task.append((c[1], c[2]))
    print_log('закончили загружать URL с BD всего загрузили '+str(len(all_links_from_base_for_task))+' урлов')
    return True


def load_log_from_bd(conn):
    print_log('начали загружать лог из базы данных')
    global all_log_from_base
    all_log_from_base = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM task_log")
    for c in cursor:
        all_log_from_base.append((c[1],c[2]))
    print_log('закончили загружать log с BD всего загрузили '+str(len(all_log_from_base))+' урлов')
    return True


def load_active_task_from_bd(conn):
    print_log('начали загружать активные таски')
    global active_task
    active_task = []

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks where active = %s", [1])
    for c in cursor:
       active_task.append(c[1])
    print_log('закончили загружать активные таски '+str(len(active_task))+' урлов')
    return True


def set_task_status (id, status, url, err='ok'):
    cursor = conn.cursor()
    cursor.execute("UPDATE `tasks` SET `complete_task_time` = %s, `active` = %s  WHERE `id` = %s",
                   (round(time.time()), 0, id))

    cursor.execute("INSERT INTO `task_log`( `url`, `time`, `status_ok`, `err`) VALUES (%s, %s, %s, %s)",
                   (url, round(time.time()), status, str(err)))
    conn.commit()


def form_new_tasks():
    load_active_task_from_bd(conn)
    print_log(f'активных тасков {len(active_task)} ')
    if len(active_task) < MINTASKQUEUE:
        load_all_links_from_bd_for_tasks(conn)
        load_log_from_bd(conn)
        cursor = conn.cursor()
        print_log('начали проверять ссылки из базы, '
                  'какие URL уже есть в активных тасках и какие уже выполнялись и есть в логах')
        for task in all_links_from_base_for_task:
           #if task[0] not in (url[0] for url in all_log_from_base):
           if task[0] not in active_task:
               if task[1] == 3:
                    cursor.execute("INSERT INTO `tasks`"
                                   "( `task_url`, `job_type_id`,`add_task_time`,`active`) VALUES (%s, %s, %s, %s)",
                                   (task[0], task[1], round(time.time()), 1))
               if task[1] == 1:
                    cursor.execute(
                        "INSERT INTO `tasks`"
                        "( `task_url`, `job_type_id`,`add_task_time`,`active`) VALUES (%s, %s, %s, %s)",
                        (task[0], task[1], round(time.time()), 1))
        conn.commit()
        print_log('закончили добавлять в базу таски на поиск ссылок')
    print_log('выходим из form_new_tasks')


class Task:
    def __init__(self):
        self.task_id  = ''
        self.task_url = ''
        self.task_type = ''
        self.all_grab_urls = []
        self.one_format = []
        self.sorted_urls = []

    def add_task(self, id, url, type):
        self.task_id = id
        self.task_url = url
        self.task_type = type

    def grab_page(self):
        print_log('в функции grab_page начинаем загружать страницу')
        proxy = {'http': 'http://' + random_proxy()}
        useragent = {'User-Agent': random_ua()}
        session = requests.Session()
        request = session.get(self.task_url, headers=useragent, proxies=proxy)
        if request.status_code == 200:
            soup = bs(request.content, 'html.parser')
            print_log('загрузили страницу %s, статус %s' % (self.task_url, request.status_code))
            self.all_grab_urls = soup.find_all('a')
            print_log('всего на странице нашли %s URL' % len(self.all_grab_urls))
            return True
        else:
            set_task_status(self.task_id, 0, self.task_url, str(request.status_code))
            print('!!! ОШИБКА !!!%s URL %s' % (str(request.status_code), self.task_url))
            return False

    def sort_and_save_grab_urls(self):
        print_log('начинаем сортировать url со сграбленные страницы всего URL = %s' % len(self.all_grab_urls))
        for url in self.all_grab_urls:
            try:
                if url.get('href').find('http') == 0:# приводим URL к эдиному формату
                    url1 = url.get('href').replace('//www.', '//')
                else:
                    dom ='https://play.google.com'
                    url1 = dom+url.get('href')
                # после приведения к единому формату складываем в один список
                if url1 not in self.one_format: self.one_format.append(url1)
            except Exception as e:
                print('ОШИБКА %s -- URL %s' % (e, url))
        print_log('проверяем URL и присваиваем им тип страницы')
        i = 0
        print_log('нашли на странице %s URL' % len(self.one_format))
        for url in self.one_format:
            i += 1
            if url.find('https://play.google.com/') == 0:
                if url.find('https://play.google.com/store/apps/details') == 0:
                    self.sorted_urls.append((url, 1)) #app details
                elif url.find('https://play.google.com/store/apps') == 0 or url.find(
                        'https://play.google.com/apps') == 0:
                    self.sorted_urls.append((url, 3))  # most priority for find new games pages
                elif url.find('https://play.google.com/store/search') == 0 and (url.find('c=apps') != -1):
                    self.sorted_urls.append((url, 3))  # most priority for find new games pages
                elif url.find('https://play.google.com/store/apps/dev') == 0:
                    self.sorted_urls.append((url, 2)) # page app developer
                else:
                    self.sorted_urls.append((url, 5)) #other internal links
        print_log('присвоили всем URL тип страницы')
        print_log('начинаем добавлять в базу новые URL со страницы')
        '''
        перебираем массив ссылок, смотрим что бы не было ссылки в базе
        добавляем новые ссылки в базу 
        добавляем запись лог базы
        отмечаем что мы выполнили таск
        '''
        arr=[]
        for url in all_links_from_base:
            arr.append(url[0])
        print_log('---загружено страниц с базы %s, а отсортировано уролов %s, пример URL %s' %
                  (str(len(arr)), str(len(self.sorted_urls)), arr[0]))
        cursor = conn.cursor()
        for url in self.sorted_urls:
            if url[0] not in arr:
                cursor.execute("INSERT INTO `all_site_urls`( `url_link`, `url_link_type_id`) VALUES (%s, %s)",
                               (url[0], url[1]))
        cursor.execute("INSERT INTO `task_log`( `url`, `time`) VALUES (%s, %s)",
                       (self.task_url, round(time.time())))
        cursor.execute("UPDATE `tasks` SET `complete_task_time` = %s, `active` = %s WHERE `id` =%s",
                       (round(time.time()),0, self.task_id))
        conn.commit()
        print_log('закончили добавлять URL со страницы в базу')

def save_apps_screens_to_bd(all_screens):
    global all_games_screens
    cursor = conn.cursor()
    for one_screen in all_screens:
        sql = "INSERT INTO `apps_screens` (`screen_url`, `gp_id`)" \
              " VALUES " \
              "(%s, %s)"
        param = (one_screen[0],one_screen[1])
        cursor.execute(sql, param)
    conn.commit()
    all_games_screens = []

def save_all_apps_data_to_base(data):
    print_log('начали добавлять в базу информацию о %s приложениях ' % len(data))
    global data_all_games
    for app_dict in data:
        cursor = conn.cursor()

        sql = "INSERT INTO `all_apps` (`app_name`, `app_url`, `app_gp_id`, `app_red_choise`, `app_dev_name`, `app_dev_link`," \
              " `app_genre`, `app_logo_small`, `app_average_rating`, `app_number_of_ratings`, `app_min_age`, `app_rek`, " \
              "`app_video`, `app_text`, `app_rat_5`, `app_rat_4`, `app_rat_3`, `app_rat_2`, `app_rat_1`, `app_update_date`," \
              " `app_size`, `app_download_count`, `app_curent_ver`, `app_req_android`, `app_inter_elements`," \
              " `app_content_price`, `dev_web_site`, `dev_mail`, `dev_address`, `app_link_download`, `app_price`, `app_genre_id`)" \
              " VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s)"
        param = (app_dict.get('app_name'), app_dict.get('app_url'), app_dict.get('app_gp_id'),
                 app_dict.get('app_red_choise'), app_dict.get('app_dev_name'), app_dict.get('app_dev_link'),
                 app_dict.get('app_genre'), app_dict.get('app_logo_small'), app_dict.get('app_average_rating'),
                 app_dict.get('app_number_of_ratings'), app_dict.get('app_min_age'),
                 app_dict.get('app_rek'), app_dict.get('app_video'), app_dict.get('app_text'),
                 app_dict.get('app_rat_5'), app_dict.get('app_rat_4'), app_dict.get('app_rat_3'),
                 app_dict.get('app_rat_2'),
                 app_dict.get('app_rat_1'), app_dict.get('app_update_date'), app_dict.get('app_size'),
                 app_dict.get('app_download_count'), app_dict.get('app_curent_ver'), app_dict.get('app_req_android'),
                 app_dict.get('app_inter_elements'), app_dict.get('app_content_price'), app_dict.get('dev_web_site'),
                 app_dict.get('dev_mail'), app_dict.get('dev_address'), app_dict.get('app_link_download'),
                 app_dict.get('app_price'), dict_gener_id.get(app_dict.get('app_genre'), 100))
        cursor.execute(sql, param)

        cursor.execute("INSERT INTO `task_log`( `url`, `time`) VALUES (%s, %s)",
                       (app_dict.get('task_url'), round(time.time())))
        cursor.execute("UPDATE `tasks` SET `complete_task_time` = %s, `active` = %s WHERE `id` =%s",
                       (round(time.time()), 0, app_dict.get('task_id')))
        print_log('сформировали новый запрос')
        conn.commit()
        print_log('закончили добавлять в базу информацию о приложении ')
        data_all_games = []

class NewApp:
    def __init__(self):
        self.one_app_data = []
        self.app_screens = []
        self.app_gp_id = ''
        self.soup = ''
        self.app_url = ''

    def grab_app_page(self,url):
        self.app_url = url
        print_log('в функции grab_app_page начинаем грабить страницу приложения')
        proxy = {'http': 'http://' + random_proxy()}
        useragent = {'User-Agent': random_ua()}
        session = requests.Session()
        request = session.get(self.app_url, headers=useragent, proxies=proxy)
        if request.status_code == 200:
            self.soup = bs(request.content, 'html.parser')
            print_log('загрузили страницу %s, статус %s' % (self.app_url, request.status_code))
            return True
        else:
            print('!!! ОШИБКА !!!%s URL %s' % (str(request.status_code), self.app_url))
            return False

    def app_page_parse(self,task_id,url):
        global data_all_games
        global all_games_screens
        apps_id = []
        cursor = conn.cursor()
        cursor.execute("SELECT `id` FROM all_apps where app_gp_id = %s", [self.app_gp_id])
        for c in cursor:
            apps_id.append(c[0])
        #print_log(apps_id)

        price = ''
        soup = self.soup
        self.app_gp_id = self.app_url.split('id=')[1]
        self.one_app_data.append(['app_name',self.soup.find_all('h1')[0].text])
        self.one_app_data.append(['app_url', self.app_url])
        self.one_app_data.append(['app_gp_id', self.app_gp_id])
        self.one_app_data.append(['task_id', task_id])
        self.one_app_data.append(['task_url', url])
        try:
            self.one_app_data.append(['app_red_choise', self.soup.find('span', class_='dMMEE').text])
        except:
            self.one_app_data.append(['app_red_choise', ''])
        try:
            self.one_app_data.append(['app_link_download', self.soup.find('meta', itemprop='url')['content']])
        except:
            self.one_app_data.append(['app_link_download', ''])
        try:
            if self.soup.find('button', class_='LkLjZd ScJHi HPiPcc IfEcue')['aria-label'] != 'Install': price = '0'
            self.one_app_data.append(['app_price', price])
        except:
            self.one_app_data.append(['app_price', ''])

        try:
            self.one_app_data.append(['app_dev_name', self.soup.find('span', class_='T32cc UAO9ie').text])
        except:
            self.one_app_data.append(['app_dev_name', ''])

        try:
            self.one_app_data.append(['app_dev_link',
                                      soup.find('span', class_='T32cc UAO9ie').a.get('href')])
        except:
            self.one_app_data.append(['app_dev_link', ''])

        try:
            self.one_app_data.append(['app_genre',
                                      soup.find('a', itemprop='genre').text.lower()])
        except:
            self.one_app_data.append(['app_genre', ''])
        try:
            self.one_app_data.append(['app_logo_small',
                                      soup.find('img', class_='T75of sHb2Xb')['src']])
        except:
            self.one_app_data.append(['app_logo_small', ''])

        try:
            self.one_app_data.append(['app_average_rating',
                                      soup.find('div', class_='pf5lIe').div['aria-label'].split(' ')[1]])
        except:
            self.one_app_data.append(['app_average_rating', ''])
        try:
            self.one_app_data.append(['app_number_of_ratings',
                                      soup.find('span', class_='AYi5wd TBRnV').span.string.replace(',', '')])
        except:
            self.one_app_data.append(['app_number_of_ratings', ''])

        try:
            self.one_app_data.append(['app_min_age', soup.find('img', class_='T75of E1GfKc')['alt'] ])
        except:
            self.one_app_data.append(['app_min_age', '' ])

        try:
            if soup.find('div', class_='bSIuKf'):
                self.one_app_data.append(['app_rek', '1'])
            else:
                self.one_app_data.append(['app_rek', '0'])
        except:
            self.one_app_data.append(['app_rek', 'False'])
        try:

            for sc_tag in soup.find_all('img', class_='T75of DYfLw'):
                try:
                    self.app_screens.append(sc_tag['data-src'])
                except:
                    self.app_screens.append(sc_tag['src'])
        except:
            self.app_screens = []
        for screen in self.app_screens:
            all_games_screens.append((screen, self.app_gp_id, apps_id))
        try:
            self.one_app_data.append(['app_video', soup.find('div', class_='TdqJUe')
                                     .button['data-trailer-url'].split('?')[0]])
        except:
            self.one_app_data.append(['app_video', ''])

        try:
            self.one_app_data.append(['app_text', soup.find('div', jsname='sngebd').text])
        except:
            self.one_app_data.append(['app_text', ''])
        try:
            self.one_app_data.append(['app_rat_5',
                                      soup.find('span', class_='L2o20d P41RMc')['style'].replace('width: ', '').replace(
                                          '%', '')])
            self.one_app_data.append(['app_rat_4',
                                      soup.find('span', class_='L2o20d tpbQF')['style'].replace(
                                          'width: ', '').replace('%', '')])
            self.one_app_data.append(['app_rat_3',
                                      soup.find('span', class_='L2o20d Sthl9e')['style'].replace(
                                          'width: ', '').replace('%', '')])
            self.one_app_data.append(['app_rat_2',
                                      soup.find('span', class_='L2o20d rhCabb')['style'].replace(
                                          'width: ', '').replace('%', '')])
            self.one_app_data.append(['app_rat_1',
                                      soup.find('span', class_='L2o20d A3ihhc')['style'].replace(
                                          'width: ', '').replace('%', '')])
        except:
            self.one_app_data.append(['app_rat_5',''])
            self.one_app_data.append(['app_rat_4',''])
            self.one_app_data.append(['app_rat_3',''])
            self.one_app_data.append(['app_rat_2',''])
            self.one_app_data.append(['app_rat_1',''])

        try:
            for s in soup.find_all('div', class_='hAyfc'):
                if s.find('div', class_='BgcNfc').text == 'Updated':
                    self.one_app_data.append(['app_update_date',s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text])
                elif s.find('div', class_='BgcNfc').text == 'Size':
                    self.one_app_data.append(['app_size',s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text])
                elif s.find('div', class_='BgcNfc').text == 'Installs':
                    self.one_app_data.append(['app_download_count', s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text.replace(',','').replace('+', '')])
                elif s.find('div', class_='BgcNfc').text == 'Current Version':
                    self.one_app_data.append(['app_curent_ver', s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text])
                elif s.find('div', class_='BgcNfc').text == 'Requires Android':
                    self.one_app_data.append(['app_req_android', s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text])
                elif 'Interactive' in s.find('div', class_='BgcNfc').text:
                    self.one_app_data.append(['app_inter_elements', s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text])
                elif 'Products' in s.find('div', class_='BgcNfc').text:
                    self.one_app_data.append(['app_content_price',  s.find('div', class_='IQ1z0d').find(
                        'span', class_='htlgb').text])
                elif 'Developer' in s.find('div', class_='BgcNfc').text:

                    so = s.find('div', class_='IQ1z0d').find('span', class_='htlgb')
                    self.one_app_data.append(['dev_web_site', so.find('a', class_='hrTbp')['href']])
                    self.one_app_data.append(['dev_mail', so.find('a', class_='hrTbp euBY6b')['href'].replace('mailto:', '')])
                    self.one_app_data.append(['dev_address', so.find_all('div')[-1].text])
        except:
            pass

        data_all_games.append(dict(self.one_app_data))

'''
дополнительные функции для работы с бд
'''
def update_id_in_sreens():
    data_from_apps = []
    data_from_screens = []
    cursor = conn.cursor()
    cursor.execute("SELECT `id`, `app_gp_id` FROM `all_apps`")
    for data in cursor:
        data_from_apps.append((data[0], data[1]))
    cursor.execute("SELECT `id`,`gp_id` FROM `apps_screens` WHERE `app_id` IS NULL")
    for data in cursor:
        data_from_screens.append((data[0], data[1]))

    cursor3 = conn.cursor()
    for data_screens in data_from_screens:
        for data_apps in data_from_apps:
            if data_screens[1] == data_apps[1]: cursor3.execute("UPDATE `apps_screens` SET `app_id` = %s "
                                                                "WHERE `id` = %s",
                                                                (data_apps[0], data_screens[0]))
    conn.commit()

def change_video_url():
    cursor = conn.cursor()
    cursor.execute("SELECT `id`, `app_video` FROM `all_apps` WHERE `app_video` LIKE '%/watch?%'")
    for c in cursor:
        "UPDATE`all_apps` SET `app_video` = %s WHERE `id` = %s",
        (c[1].replace('/watch?v=','/embed/'), c[0])



if __name__ == '__main__':
    pass