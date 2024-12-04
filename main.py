import requests, math, sys, os
import pandas as pd
import time, datetime
import json
from json.decoder import JSONDecodeError
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from copy import deepcopy
from collections import OrderedDict, defaultdict
from pathlib import Path
from numpy import where

# written by Shahzod on 25 Nov 2023

'''
Ushbu faylda multiprocessing jarayoni uchun zarur klass va funksiyalar, asosiy konstantalar qiymati yozilgan
'''

# To prevent SettingWithCopyWarning when dropping transactions not having the error code "CONFIRM"
pd.options.mode.chained_assignment = None

SITE_SETTLEMENTS_CENTRE = 'http://nnn'
SITE_ANOR = 'http://zzz'
PAGE_SIZE = 10000
MAX_THREADS = 24
PURPOSE_BOUNDARY = 10**8
COLUMNS = ['BANK_DT', 'ACCOUNT_DT', 'BANK_CR', 'ACCOUNT_CR', 'SUMMA', 'CLIENT_DT', 'CLIENT_CR', 'PURPOSE', 'PURPOSE_TEXT', 'TIME']
DOWNLOADS_FOLDER = Path('C:\\Users\\msd13\\Downloads')
RESULTS_FOLDER = Path('C:\\Users\\msd13\\Documents\\Test Liquidity')
# DOWNLOADS_FOLDER = Path('C:\\Users\\Karimdjanova_X\\Downloads')
# RESULTS_FOLDER = Path('D:')


class Site:
    '''Ikkala sayt to'g'risidagi asosiy ma'lumotlarni saqlovchi klass.
       i = 0: Центр расчетов (korr.schot),
       i = 1: Anor
    '''

    def __init__(self, i, date=None):
        self.i = i
        self.url = [
            SITE_SETTLEMENTS_CENTRE, 
            SITE_ANOR
        ][i]
        self.payload = {
            'LoginForm[username]': ['aaa', 'bbb'][i],
            'LoginForm[password]': 'yyy',
            'yt0': 'Вход'
        }
        self.login_url = [
            'http://aaa',
            'http://bbb'
        ][i]
        self.payments_url = self.url + [
            'kkk',
            'jjj'
        ][i]
        self.keys_list = [
            ['BANK_A', 'ACCOUNT_A', 'BANK_B', 'ACCOUNT_B', 'SUMMA', 'NAME_A', 'NAME_B', 'DESTINATION', 'PURPOSE', 'PROCESS_TIMESTAMP'],
            ['PAYER_BANK', 'PAYER_ACCOUNT', 'PAYEE_BANK', 'PAYEE_ACCOUNT', 'AMOUNT', 'PAYER_NAME', 'PAYEE_NAME', 'PURPOSE_CODE', 'PURPOSE_TEXT', 'TIME']
        ][i]
        self.page = 1
        self.date = date
        self.cookies = None
        self.page_size = 10
        self.dt_or_cr = None
        self.side = None

        if i:
            self.PARAMS = {
                # 'data': date,
                'page': self.page,
                'size': self.page_size,
                'summa_compare': 1,
                'and_or': 1,
                'state': 7
            }
            self.clearing = None
        else:
            self.PARAMS = {
                'error': 2,
                'pageSize': self.page_size,
                'date': date
            }

    def get_cookie(self):
        '''Saytga kirib, avtorizatsiya cookie'sini bilib olish'''
        
        try:
            with requests.Session() as s:
                response = s.post(self.login_url, data=self.payload)
                if not response.ok:
                    print('ERROR: CANNOT LOGIN INTO' + self.url)
                    exit()

                auth_key = response.headers['Set-Cookie']
                cookie = auth_key[auth_key.find('=')+1:auth_key.find(';')]
                self.cookies = {'YII_SESSION': cookie}
        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print("\nCM Client ishlayotganligiga ishonch hosil qiling!\n")
            print_err_info(err, exc_tb)

    def get_num_pages(self):
        '''Get the number of transactions'''

        try:
            cl_or_date = self.clearing  if self.i else self.date
            content = get_transactions(self)
            if content:
                num_transactions = int(content[0]['COUNT_ALL' if self.i else 'CNTALL'])
                num_pages = math.ceil(num_transactions / PAGE_SIZE)
                print(cl_or_date, self.side, 'The number of transactions', num_transactions, 'The number of pages', num_pages)

                # num_pages = 1
                self.PARAMS['size' if self.i else 'pageSize'] = PAGE_SIZE

                return num_pages

            else:
                print(cl_or_date, 'NO OPERATIONS for ' + self.side)
                
        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print_err_info(err, exc_tb)
                
    def get_session_details(self):
        '''Joriy sana uchun kliring sessiyalari va kalendar kunlarni aniqlash'''
        # only for ANOR

        try:
            URL = self.url + 'getSessionByDate'
            PARAMS = {
                'data': self.date
            }

            response = requests.post(url=URL, data=PARAMS, cookies=self.cookies)
            content = get_content(response)

            session_details = dict()
            # content := [
            # {'ID': '20237311137', 'NAME': 'Операционный день: 28.11.2023, сессия № 1 - с 27.11.2023 16:32:34 по 28.11.2023 08:32:08'},
            # {'ID': '20237311138', 'NAME': 'Операционный день: 28.11.2023, сессия № 2 - с 28.11.2023 08:32:08 по 28.11.2023 19:32:59'},
            # {'ID': '20237321140',  'NAME': 'Операционный день: 29.11.2023, сессия № 1 - с 28.11.2023 19:32:59 по 29.11.2023 08:32:06'}]
            for session in content:
                session_name = session['NAME']
                oper_date = session_name[19:29]
                if oper_date == self.date:
                    session_details[session['ID']] = [session_name[46:56], session_name[69:79]]

            return session_details

        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print_err_info(err, exc_tb)

    def get_branch_remainders(self):
        '''Banklar korschotlari qoldiqlari va oborotlar to'g'risidagi ma'lumotni olish'''
        # only for Центр расчетов

        try:
            URL = self.url + 'branchRemainders'
            PARAMS = {
                'date': self.date
            }

            response = requests.post(url=URL, data=PARAMS, cookies=self.cookies)
            content = get_content(response)
            
            if not content:
                return

            REMAINDERS_COLUMNS = ['BANK', 'BEGIN_', 'DEBET_', 'CREDIT_', 'END_']
            remainders = [list(map(x.__getitem__, REMAINDERS_COLUMNS)) for x in content]
            df = pd.DataFrame(remainders, columns=REMAINDERS_COLUMNS).drop(index=0).set_index('BANK').astype(float).div(10**11)
            #                                                        Markaziy bank olib tashlandi                  tiyindan mlrd.ga
            df = df.drop('069', errors='ignore') # Milliy kliring markazini olib tashlash

            print("Banklar korschot qoldiqlari va oborotlar yuklab olindi")
            return df 

        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print_err_info(err, exc_tb)

    def get_system_defines(self):
        '''Kun yopilish vaqtini aniqlash (FIRST CUT)'''
        # only for Центр расчетов
        try:
            URL = self.url + 'systemDefines'

            response = requests.post(url=URL, cookies=self.cookies)
            content = get_content(response)
            system_defines = {detail['ALIAS_']:detail['VALUE_'] for detail in content}
            # print(system_defines)
            try:
                return datetime.datetime.strptime(system_defines['dfClientEnd'], '%d.%m.%Y %H:%M:%S').time()
            except Exception as err:
                print('Could not get the time for the FIRST CUT. Using 17:01')
                return datetime.time(17, 1)

        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print_err_info(err, exc_tb)

    def get_bank_codes(self):
        try:
            URL = self.url + 'branchState'

            response = requests.post(url=URL, cookies=self.cookies)
            content = get_content(response)
            bank_codes = [x['BANK_CODE'] for x in content]
            bank_codes.remove('001')
            
            return bank_codes
        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print_err_info(err, exc_tb)


class Runner:
    '''ProcessPoolExecutor'ga parametr sifatida kiritish uchun saytlar to'g'risidagi ma'lumotlardan iborat klass.
        Debit va kredit oborotlar, betlar uchun alohida-alohida Site klassi namunalarini hosil qilinadi.
        dt_or_cr = 0: debit,
        dt_or_cr = 1: credit
    '''
    
    def __init__(self, site, dt_or_cr):
        self.site = deepcopy(site)
        self.site.dt_or_cr = [
            ['brancha', 'branchb'], 
            ['bank_a', 'bank_b']
        ][site.i][dt_or_cr]
        self.site.side = ['DT', 'CR'][dt_or_cr]
        self.site.PARAMS[self.site.dt_or_cr] = '001'
        self.num_tries = 0

    def make_chunks(self, page):
        site_bite = deepcopy(self.site)
        site_bite.PARAMS['page' if site_bite.i else 'pageNumber'] = page
        site_bite.page = page
        return site_bite


class CommercialBankRunner(Runner):

    def __init__(self, site, dt_or_cr, bank_code):
        super().__init__(site, dt_or_cr)
        self.site.PARAMS[self.site.dt_or_cr] = bank_code


def get_content(response):
    '''Server javobi muvaffaqiyatli bo'lsa, undan content'ni ajratib olish'''

    if not response.ok:
        print('NO SUCCESSFUL RESPONSE')
        return
    
    response = response.json()
    return response['content']


def get_transactions(site):
    '''Serverga request yuborib, javob olish'''
    
    response = requests.post(url=site.payments_url, data=site.PARAMS, cookies=site.cookies)
    if site.PARAMS['size' if site.i else 'pageSize'] != 10:
        site_date = site.clearing if site.i else ''
        print(site_date, site.side, "Got the page", site.page)
        # print(site.PARAMS[site.dt_or_cr], "Got the page", site.page)

    return get_content(response)


def get_by_branch(runner):
    '''ProcessPoolExecutor parallel ravishda bajaruvchi funksiya'''

    site = runner.site
    num_pages = site.get_num_pages()
    if num_pages:
        # Har bir bet uchun alohida Site klassi namunasini yaratish
        chunks = [runner.make_chunks(page+1) for page in range(num_pages)]

        try:
            threads = min(MAX_THREADS, num_pages)
            # Serverga parallel requestlar yuborish
            with ThreadPoolExecutor(max_workers=threads) as executor:
                results_map = executor.map(get_transactions, chunks)

            # Javoblardan kerakli qismini ajratib olish
            transactions = [list(map(x.get, site.keys_list)) for cont in results_map for x in cont]
            df = pd.DataFrame(data=transactions, columns=COLUMNS)
        # except JSONDecodeError:
        #     if runner.num_tries < 10:
        #         runner.num_tries += 1
        #         print('JSONDecodeError occurred. Retrying', runner.num_tries)
        #         get_by_branch(runner)
        #     else:
        #         print('Program exceeded the number of maximum tries')
        #         raise JSONDecodeError
        except Exception as err:
            exc_tb = sys.exc_info()[2]
            print_err_info(err, exc_tb)
    else:
        df = pd.DataFrame(columns=COLUMNS)

    runner.num_tries = 0
    return site, df


def print_err_info(err, exc_tb):
    e_name = err.__class__.__name__
    e_filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(e_name, "occurred:", err, "ON THE LINE", exc_tb.tb_lineno, 'of', e_filename, flush=True)
    exit()
