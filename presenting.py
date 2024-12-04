import win32com.client as win32
import pandas as pd 
import numpy as np
from pivoting import run_excel
from pathlib import Path
import xlsxwriter, os
from get_data import DOWNLOADS_FOLDER, RESULTS_FOLDER

# written by Shahzod on 5 Dec 2023

'''Faktorlar jamlangan "pivot table"lardan faktorlarni olib, shablon faylga qo'yishga tayyorlash'''

FACTOR_NAMES = ['Клиринг','Қ/х жамғармаси','МБ Депозит фоиз','МБ Депозит','МБ кредит',
                'МБ хўжалик операциялари','Нақд пул','ФОР','ФРРУ','Ҳукумат','Бошқа',
                'Қимматбаҳо металлар билан ҳисоб-китоблар', 'ГЦБ', 'Биржа билан операциялар',
                'Овернайт депозит','Депозит аукциони','РЕПО аукциони','РЕПО овернайт',
                'Своп аукциони','Своп овернайт', 'Кредит аукциони','Кредит овернайт', 
                'Овернайт депозит','Депозит аукциони', 'МБ облигациялари', 'Банклараро РЕПО']

EXCEPTIONS = ['Овернайт депозит','Депозит аукциони','РЕПО аукциони','РЕПО овернайт', 'Кредит аукциони','Кредит овернайт',
              'Своп аукциони','Своп овернайт','Овернайт депозит','Депозит аукциони', 'МБ облигациялари']

TEMPLATE_FACTORS = ['Ҳукумат','ФРРУ','ФОР','МБ Депозит','МБ кредит','МБ облигациялари',
                    'Овернайт депозит','Депозит аукциони','РЕПО овернайт','РЕПО аукциони',
                    'Своп овернайт','Своп аукциони', 'Кредит аукциони','Кредит овернайт', 
                    'ГЦБ', 'Банклараро РЕПО', 'Қимматбаҳо металлар билан ҳисоб-китоблар',
                    'Нақд пул','Қ/х жамғармаси','Биржа билан операциялар']

DROP_NAMES = ['999-Марказий банк', 'Общий итог']


def presenting(dfs, oper_date, prelimenary, first_cut, third_cut):

# if __name__ == '__main__':
#     # FOR TESTING
#     oper_date = '14.02.2024'
#     prelimenary = 0
#     first_cut = 0 
#     third_cut = 0
#     f_path = Path('C:\\Users\\msd13\\Documents\\Test Liquidity\\кунлик корр. счет 2024\\02 Февраль\\Февраль')
#     f_name =  f'Liquidity factor {oper_date}.xlsx'
#     dfs = run_excel(f_path / f_name)

    net = dfs['Net']
    df_net = pd.DataFrame(list(net[4:]), columns=net[3]).set_index('Factors')
    df_net.drop(['Общий итог'], axis=0, inplace=True)

    total_net = df_net['Общий итог']
    blank_df = pd.DataFrame(index=TEMPLATE_FACTORS)
    total_net = blank_df.join(total_net).fillna(0).reset_index()

    df_net = df_net.drop(DROP_NAMES, axis=1).round().astype(int)
    mask_net = df_net.apply(lambda x: (x!=0) & ((abs(x)>=9) | (x.name in EXCEPTIONS)), axis=1)

    govs = []
    
    # Extract government operations
    for side in ('DT', 'CR'):
        starting = ending = 0
        tuples = dfs['Pivot' + side]

        for n, r in enumerate(tuples):
            if r[0] == 'Ҳукумат':
                starting = n + 1
                break

        for k, r in enumerate(tuples[starting+1:]):
            if r[0] in FACTOR_NAMES:
                ending = k + starting
                break
        else:
            ending = k

        if starting:
            hukumat = list(tuples[starting:ending+1])
            df_gov = pd.DataFrame(hukumat, columns=tuples[3]).set_index('Factors').fillna(0).div(10**9).round().astype(int)
            df_gov.drop(['Республика бюджети'], axis=0, inplace=True, errors='ignore')
            
            if side == 'CR':
                df_gov = -df_gov

            govs.append(df_gov)

    df_govs = pd.concat(govs, sort=False) if len(govs) else pd.DataFrame(columns=df_net.columns)
    df_govs.drop(DROP_NAMES, axis=1, inplace=True, errors='ignore')
    bool_mask = df_govs.apply(lambda cell: abs(cell)>=9, axis=1)

    all_banks = dict()
    for name in df_net.columns:
        bank_gov = df_govs[name][bool_mask[name]]
        visible_gov = bank_gov.sum()

        try:
            # Remove the effect of visible government payments from the total government effect
            remaining_gov = df_net.loc['Ҳукумат', name] - visible_gov
            if abs(remaining_gov) >= 9:
                df_net.loc['Ҳукумат', name] = remaining_gov
            else:
                mask_net.loc['Ҳукумат', name] = False
        except:
            pass

        bank_net = df_net[name][mask_net[name]]

        # Sort by absolute value in decending order
        # bank_net = bank_net.iloc[(-bank_net.abs()).argsort()]
        # If pandas >= V_1.1.0:
        bank_net = bank_net.sort_values(ascending=False, key=abs)
        
        # Firstly sort positive, then negative values
        bank_gov = pd.concat((bank_gov[bank_gov > 0].sort_values(ascending=False), 
                              bank_gov[bank_gov < 0].sort_values()))

        # all_factors = pd.concat([bank_net, bank_gov]).astype(str).reset_index()
        all_factors = [df.astype(str).reset_index() for df in [bank_net, bank_gov]]
        bank_code = name[:3]

        # all_banks[bank_code] = [all_factors[col].str.cat(sep='\n') for col in all_factors]
        all_banks[bank_code] = [[[df[col].str.cat(sep='\n') for col in df] for df in all_factors]]

    factors_df = pd.DataFrame.from_dict(all_banks, orient='index')
        
    remainders = pd.read_csv(DOWNLOADS_FOLDER / f'Remainders_{oper_date}.csv', dtype={'BANK': object}).set_index('BANK')
    remainders['DIFFERENCE'] = remainders['END_'] - remainders['BEGIN_']

    combined = remainders.join(factors_df)#.fillna('')

    # Fill na with nested empty lists
    combined[0] = combined[0].apply(lambda d: d if isinstance(d, list) else [[], []])
    combined = combined.explode(0).fillna('')
    combined[[0, 1]] = pd.DataFrame(combined[0].tolist(), index=combined.index)

    workbook = xlsxwriter.Workbook(DOWNLOADS_FOLDER / f'Results_liquidity_{oper_date}.xlsx')
    wrap_format = workbook.add_format({'text_wrap': True})
    for sheet_name, df in zip(['By_bank', 'Total'], [combined, total_net]):
        worksheet = workbook.add_worksheet(sheet_name)

        for col, name in enumerate(df):
            for row, val in enumerate(df[name]):
                worksheet.write(row, col, val, wrap_format)

    worksheet = workbook.add_worksheet('EKS')
    val = f'ЕКС (23402): {dfs.get("EKS_CR", 0)} млрд. сўм йиғиб олган, {dfs.get("EKS_DT", 0)} млрд. сўм харажат қилган. Қолдиқ:  млрд. сўм (Кун бошига  млрд. сўм)'
    birja = f'Валюта биржаси (21596): банклар биржага {dfs.get("VALUTA_CR", 0)} млрд. сўм ўтказган, биржадан {dfs.get("VALUTA_DT", 0)} млрд. сўм қайтган. Қолдиқ:  млрд. сўм (Кун бошига  млрд.сўм). Минфин конвертация қилган:  млн. долл. ( млрд. сўм)'
    worksheet.write(0, 0, val, wrap_format)
    worksheet.write(1, 0, birja, wrap_format)

    workbook.close()

    # Excel'da yozilgan makrosdan foydalanamiz
    print('Writing factors in template')
    if os.path.exists('MacroLiquidity.xlsm'):
        xl = win32.Dispatch('Excel.Application')
        wb = xl.Workbooks.Open(os.path.abspath('MacroLiquidity.xlsm'))
        xl.Application.Run('MacroLiquidity.xlsm!ExtractData.Liquidity_Anor', oper_date, prelimenary, first_cut, third_cut)
        # xl.Application.Save()
        # xl.Application.Quit()
        wb.Close()
        del xl


# if __name__ == '__main__':
#     # FOR TESTING
#     oper_date = '26.12.2023'
#     f_path = Path('C:\\Users\\msd13\\Documents\\Test Liquidity\\кунлик корр. счет 2023\\12 Декабрь\\Декабрь')
#     # f_path = Path('C:\\Users\\msd13\\Documents\\Test Liquidity\\кунлик корр. счет 2023\\10 Октябрь\\Октябрь')
#     # f_path = Path('C:\\Users\\msd13\\Documents\\Test Liquidity\\кунлик корр. счет 2023\\11 Ноябрь\\Ноябрь')
#     f_name =  f'Liquidity factor {oper_date}.xlsx'
#     dfs = run_excel(f_path / f_name)
#     presenting(dfs, oper_date, 0)
