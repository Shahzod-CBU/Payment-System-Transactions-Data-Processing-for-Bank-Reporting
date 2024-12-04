from combine import combine_databases
from pivoting import run_excel
import pandas as pd
import time, sys, os
from pathlib import Path
from get_data import DOWNLOADS_FOLDER, RESULTS_FOLDER
from presenting import presenting
import re, datetime

# written by Shahzod on 4 Dec 2023

'''Faktorlarni aniqlash va excelga tayyorlash'''

PURPOSES = {
    '08201': 'дивиденд тўлаган',
    '08101': 'солиқ тўлаган',
    '08102': 'солиқ тўлаган',
    '09570': 'давлат божи тўлаган'
}

AGROBANK = '004'

ACCOUNTS = {
    'aniqlanish_jarayoni': '17305',
    'xujalik_hisobidagilar': '21596',
    'EKS': '23402',
    'kliring': '27480',
    'tmz_tulanadigan': '29802',
    'boshqa_majburiyatlar': '29896'
}

MB_BANKOMAT = '29896000400009001805'
MUNIS_KLIRING = '27480000100009001001'
QIMMATBAHO_METALLAR = '29802000900000014573'
NOBANK_MOLIYA = {
    '04212053': 'Фуқароларни омонатларини кафолатлаш фонди',
    '05141951': 'Ипотекани қайта молиялаштириш компанияси',
    '00790352': 'Қ/х дав. томон. қўллаб-қув. жамғармаси'
}

SECURITIES = ['21596000600447893002', '21596000500447893007']
SECONDARY_LIST = ['dilerlararo REPO', 'иккиламчи', 'ikkilamchi', 'вторичном', 'II част', 
                  'ВТОРОЙ ЧАСТИ', 'второй части', '2 кисми', 'II этап']

BUDJET_ISH_HAQI = ['23108', '23212', '23214', '23110']
PENSIYA = ['22630', '22632', '23112']
MINFIN_KREDITI = ['21604', '21610', '22004', '22010']
MINFIN_DEPOZITI = ['20602', '20603']
HARBIYLAR = ['21506', '23404']

MONETARY_OPER = {
    'DEPOO': 'Овернайт депозит', 'DEPOA': 'Депозит аукциони',
    'REPOO': 'РЕПО овернайт', 'REPOA': 'РЕПО аукциони',
    'REPOI': 'РЕПО овернайт', 'CREDI': 'Кредит овернайт',
    'SWAPO': 'Своп овернайт', 'SWAPA': 'Своп аукциони',
    'CREDO': 'Кредит овернайт', 'CREDA': 'Кредит аукциони'
}

MONTH_NAMES = {
    '01': 'Январь', '02': 'Февраль', '03': 'Март', '04': 'Апрель', '05': 'Май', '06': 'Июнь', 
    '07': 'Июль', '08': 'Август', '09': 'Сентябрь', '10': 'Октябрь', '11': 'Ноябрь', '12': 'Декабрь'
}

def get_factor(r, side, reverse_side):
    # r - current row of the dataframe
    is_dt = side=='DT'
    is_cr = side=='CR'
    
    if r[side] == ACCOUNTS['EKS']:
        if r[reverse_side] in BUDJET_ISH_HAQI:
            return ['Бюджет ходимлари маоши', "Ҳукумат"]
        if r[reverse_side] in PENSIYA:
            return ['Пенсия', "Ҳукумат"]
        if r[reverse_side] in MINFIN_KREDITI:
            return ['Минфин кредити', "Ҳукумат"]
        if r[reverse_side] in MINFIN_DEPOZITI:
            return ['Минфин депозити', "Ҳукумат"]
        
        client_code = r['ACCOUNT_' + reverse_side][9:17]
        is_mf = 'Минфин ' if is_dt else ''
        to = 'га ' if is_dt else ' '
        purpose = PURPOSES.get(r['PURPOSE'], '')
        try:
            return [is_mf + clients[client_code] + to + purpose, "Ҳукумат"]
        except:
            try:
                return [is_mf + r['CLIENT_' + reverse_side] + to + purpose, "Ҳукумат"]
            except:
                pass

    if r['CR'] == ACCOUNTS['boshqa_majburiyatlar']:
        try:
            if r['DT'] == ACCOUNTS['boshqa_majburiyatlar'] if is_dt else r['DT'] == ACCOUNTS['aniqlanish_jarayoni']:
                # # Agrobank uchun monetar operatsiya 2-qismi turini to'lov maqsadidan topamiz
                # if is_cr and r['BANK_DT'] == AGROBANK:
                    p = r['PURPOSE_TEXT']
                    num = p.index('№')
                    return [MONETARY_OPER[p[num+1:num+6]]]*2
                # return [mon_oper.loc[r['ACCOUNT_' + reverse_side], 'Operation']]*2
        except:
            return ['']*2
                
        if is_cr:
            if r['DT'] == ACCOUNTS['kliring'] and r['ACCOUNT_CR'] == MB_BANKOMAT:
                return ["МБ банкоматларидан ечилган пуллар учун", "Нақд пул"]
            if r['ACCOUNT_DT'] == MUNIS_KLIRING:
                return ["МУНИС тизими орқали тўловлар", "Ҳукумат"]

    if r[side] == '21508':
        try:
            return [NOBANK_MOLIYA[r['ACCOUNT_' + side][9:17]], 'Ҳукумат']
        except:
            return ['Нобанк молия институтлари', 'Ҳукумат']

    if r[side] in HARBIYLAR and r[reverse_side] in BUDJET_ISH_HAQI:
        return ['Ҳарбийлар пенсияси ва маоши', "Ҳукумат"]

    try:
        return [bal_acc.loc[r[side], 'Factor1'], bal_acc.loc[r[side], 'Factor2']]
    except:
        if r[side] == ACCOUNTS['xujalik_hisobidagilar']:
            try:
                return [subs.loc[r['ACCOUNT_' + side][9:17], 'Factor1'], 'МБ хўжалик операциялари']
            except:
                try:
                    acc = r['ACCOUNT_' + side]
                    if (acc==SECURITIES[0] and (secondary_gsb.search(r['PURPOSE_TEXT']) or (is_cr and not is_gsb))) or (
                        acc==SECURITIES[1] and (secondary_ocb.search(r['PURPOSE_TEXT']) or (is_cr and not is_ocb))):
                        return ['Банклараро РЕПО']*2
                    return [birja.loc[acc[9:], 'Factor2']]*2
                except:
                    if r[reverse_side] in BUDJET_ISH_HAQI:
                        return ['Ҳарбийлар пенсияси ва маоши', "Ҳукумат"]
                    return ['Ҳарбий', "Ҳукумат"]

        if r['DT'] == ACCOUNTS['tmz_tulanadigan']:
            if r['ACCOUNT_DT'] == QIMMATBAHO_METALLAR:
                return ["Қимматбаҳо металлар билан ҳисоб-китоблар"]*2
            else:
                return ['ТМЗ учун тўланадиган', 'МБ хўжалик операциялари'] 

    return ['']*2


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError:
        print('Cannot delete the file', filename)
        pass


def analyse(oper_date, prelimenary, gsb, ocb, without_anor, first_cut, third_cut, first_cut_time):
    global bal_acc, birja, subs, mon_oper, clients, banks, secondary_gsb, secondary_ocb, is_gsb, is_ocb

# if __name__ == '__main__':
#     oper_date = '12.02.2024'
#     prelimenary = gsb = ocb = without_anor = 0
    is_gsb = gsb
    is_ocb = ocb

    start_time = time.time()

    money_return = ['pul mablag`larini qaytarish', 'pul mablag`ini qaytarish']
    secondary_gsb = re.compile('|'.join(SECONDARY_LIST + money_return*(not is_gsb)))
    secondary_ocb = re.compile('|'.join(SECONDARY_LIST + money_return*(not is_ocb)))

    # Load Factor sources

    PATH = Path.cwd() 
    FACTORS = PATH / 'factors.xlsx'
    dd, mm, yy = oper_date.split('.')
    mname = MONTH_NAMES[mm]
    # 'кунлик корр. счет 2023/12 Декабрь/Декабрь'
    results_path = Path(RESULTS_FOLDER / f'кунлик корр. счет {yy}/{mm} {mname}/{mname}')
    results_path.mkdir(parents=True, exist_ok=True)

    WORKBOOK = results_path / f'Liquidity factor {oper_date}.xlsx'

    sheet_params = dict(header=0, dtype='object')

    bal_acc = pd.read_excel(FACTORS, sheet_name='BalanceAccount', header=0, index_col='Account')
    bal_acc.index = bal_acc.index.astype('str')

    birja = pd.read_excel(FACTORS, sheet_name='Birja', **sheet_params)
    birja.set_index('ClientCode', inplace=True)

    subs = pd.read_excel(FACTORS, sheet_name='Subsidiaries', **sheet_params)
    subs.set_index('ClientCode', inplace=True)

    mon_oper = pd.read_excel(FACTORS, sheet_name='MonetaryOperations', **sheet_params)
    mon_oper.set_index('Account', inplace=True)

    clients = pd.read_excel(FACTORS, sheet_name='Clients', **sheet_params)
    clients = dict(zip(clients.Code, clients.Name))

    banks = pd.read_excel(FACTORS, sheet_name='Banks', **sheet_params)

    # if is_issued:
    #     ocb = pd.read_excel('ocb_27.12.2023.xlsx', header=None, names=['BANK', 'SUMMA'], dtype='object').astype({'SUMMA': 'float64'})

    tur_provod = ['DT', 'CR']
    # n = 1

    if without_anor:
        # Without ANOR results:
        databases = [pd.read_csv(DOWNLOADS_FOLDER / f'Liquidity_{oper_date}_{side}.csv', dtype='object').astype({'SUMMA':float}) 
                    for side in ['DT', 'CR']]
        for df in databases:
            df['PLATFORM'] = 'Центр расчетов'
    else:
        # ANOR va korschotni birlashtirish
        databases = combine_databases(oper_date)

    try:
        from tqdm import tqdm
        tqdm.pandas()

        writer = pd.ExcelWriter(WORKBOOK, engine='xlsxwriter')

        # Faktorlarni aniqlash
        for n, df in enumerate(databases):
            side = tur_provod[n]

            ####### Get operations done after the first cut #######
            if third_cut:
                df['TIME'] = pd.to_datetime(df['TIME']).dt.time
                interbank = df.loc[df['TIME'] >= first_cut_time]
                interbank.drop(['PURPOSE', 'PLATFORM'], axis=1, inplace=True)
                interbank.sort_values('TIME', inplace=True)
                interbank.to_excel(DOWNLOADS_FOLDER / f'AfterCUT_{oper_date}_{side}.xlsx', index=False)
            df.drop('TIME', axis=1, inplace=True)
            #######################################################

            print('Summerizing factors for ' + side)
            reverse_side = tur_provod[1-n]
            df['DT'] = df['ACCOUNT_DT'].str[0:5]
            df['CR'] = df['ACCOUNT_CR'].str[0:5]
            # df['Factors'] = df.apply(get_factor, args=(side, reverse_side), axis=1)
            df['Factors'] = df.progress_apply(get_factor, args=(side, reverse_side), axis=1)
            df[['Factor1', 'Factor2']] = pd.DataFrame(df['Factors'].tolist(), index=df.index)
            df.drop('Factors', axis=1, inplace=True)
            df = pd.merge(df, banks, left_on='BANK_'+reverse_side, right_on='BankCode').drop(columns=['BankCode', 'BankName'])
            # df.sort_values(by='SUMMA', ascending=False, inplace=True)
            df.rename({'SUMMA': 'SUMMA_' + side, 'BankNumbered': 'BANK'}, axis=1, inplace=True)
            print(f'Writing into the {side} excel sheet...')
            df.to_excel(writer, sheet_name=side, index=False)    

    except Exception as err:
        exc_tb = sys.exc_info()[2]
        e_name = err.__class__.__name__
        e_filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e_name, "occurred:", err, "ON THE LINE", exc_tb.tb_lineno, 'of', e_filename, flush=True)
        exit()
    finally:
        print('Saving the workbook...')
        writer.close()

    dfs = run_excel(WORKBOOK)
    # dfs = run_excel(WORKBOOK, oper_date)
    presenting(dfs, oper_date, prelimenary, first_cut, third_cut)

    temp_files = [
        'Liquidity_{}_DT.csv', 'Liquidity_{}_CR.csv',
        'Remainders_{}.csv', 'Results_liquidity_{}.xlsx'
    ]

    if not (prelimenary or first_cut):
        temp_files += ['ANOR_{}_DT.csv', 'ANOR_{}_CR.csv', 'Total_{}.json']

    print('Deleting temporary files...')
    for filename in temp_files:
        silentremove(DOWNLOADS_FOLDER / filename.format(oper_date))
    
    seconds = time.time() - start_time
    print('Time taken:', time.strftime("%H:%M:%S", time.gmtime(seconds)), '\n') 
