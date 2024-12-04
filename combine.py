import pandas as pd
import json
from get_data import DOWNLOADS_FOLDER

# written by Shahzod on 2 Dec 2023

'''ANOR va Центр расчетов ma'lumotlarini birlashtirish'''

def combine_databases(oper_date):
    # oper_date = '28.11.2023'

    # Read datafiles

    PATH = DOWNLOADS_FOLDER

    dt_a = pd.read_csv(PATH / f'ANOR_{oper_date}_DT.csv', dtype='object').astype({'SUMMA':float})
    cr_a = pd.read_csv(PATH / f'ANOR_{oper_date}_CR.csv', dtype='object').astype({'SUMMA':float})
    dt_k = pd.read_csv(PATH / f'Liquidity_{oper_date}_DT.csv', dtype='object').astype({'SUMMA':float})
    cr_k = pd.read_csv(PATH / f'Liquidity_{oper_date}_CR.csv', dtype='object').astype({'SUMMA':float})

    with open(PATH / f'Total_{oper_date}.json') as json_file:
        check_totals = json.load(json_file)

    changes_ANOR = [round(check_totals[k]['DT'] - check_totals[k]['CR'], 2) for k in check_totals.keys()]

    # Clearing transactions of ANOR effecting overall liquidity
    liq_increase = lambda df: (df['ACCOUNT_DT']=='19997000800009001888') & (df['ACCOUNT_CR']=='17480000900009001001')
    liq_decrease = lambda df: (df['ACCOUNT_DT']=='27480000300009001001') & (df['ACCOUNT_CR'].isin(['29896000300009001888', '19997000800009001888']))

    increases = dt_k.loc[liq_increase(dt_k)]['SUMMA']
    decreases = dt_k.loc[liq_decrease(dt_k)]['SUMMA']

    diff_Liq = round(increases.sum() - decreases.sum(), 2)
    diff_ANOR = round(sum(changes_ANOR), 2)

    if diff_Liq == diff_ANOR:
        # Drop clearing transactions of ANOR in Liquidity databases
        dt_k = dt_k.drop([*increases.index, *decreases.index])
        increases = cr_k.loc[liq_increase(cr_k)]['SUMMA']
        decreases = cr_k.loc[liq_decrease(cr_k)]['SUMMA']
        cr_k = cr_k.drop([*increases.index, *decreases.index])

        dt_k['PLATFORM'] = 'Центр расчетов'
        cr_k['PLATFORM'] = 'Центр расчетов'
        dt_a['PLATFORM'] = 'ANOR'
        cr_a['PLATFORM'] = 'ANOR'

        # Combine ANOR and Liquidity
        dt = pd.concat([dt_k, dt_a], sort=False, ignore_index=True)
        cr = pd.concat([cr_k, cr_a], sort=False, ignore_index=True)
        print('ANOR and "Центр расчетов" data have been combined')

        return dt, cr

    else:
        print("XATOLIK! ANOR kliring summalari va korschyotdagi mos operatsiyalar summasi orasida FARQ bor.")
        def pretty_float(nums):
            return [f'{x:,.4f}' for x in nums]
        print("ANOR kliring summalari:", *pretty_float(changes_ANOR))
        print("Korschyotdagi summalar:", 
            *pretty_float(increases.values), 
            *pretty_float(-decreases.values))
        exit()

