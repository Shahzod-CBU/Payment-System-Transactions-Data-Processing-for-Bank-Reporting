from get_data import *
from show_calendar import create_calendar, grad_date
from analyse import analyse

# written by Shahzod on 2 Dec 2023

'''Korschotdan operatsiyalarni yuklab olish va analizni davom ettirish'''

if __name__ == '__main__':
    start_time = time.time()
    # oper_date = '13.12.2023'

    # Get the operation date with the Calendar picker
    root = create_calendar(True)
    oper_date, prelimenary, gsb, ocb, without_anor, first_cut, third_cut = grad_date()
    first_cut_time = None
    print(oper_date)

    try:
        root.destroy()
    except:
        # Chiqish tugmasi bosilganda
        exit()

    if not without_anor:
        # ANOR ma'lumotlari mavjudligini tekshirish
        dt_a = DOWNLOADS_FOLDER / f'ANOR_{oper_date}_DT.csv'
        cr_a = DOWNLOADS_FOLDER / f'ANOR_{oper_date}_CR.csv'

        if not (dt_a.exists() and cr_a.exists()):
            print("Tanlangan sana uchun ANOR platformasidan ma'lumotlar yuklab olinganiga ishonch hosil qiling!")
            exit()

    dt_k = DOWNLOADS_FOLDER / f'Liquidity_{oper_date}_DT.csv'
    cr_k = DOWNLOADS_FOLDER / f'Liquidity_{oper_date}_CR.csv'

    korschot = Site(0, oper_date)

    print("Korschotga kirish...")
    korschot.get_cookie()
    first_cut_time = korschot.get_system_defines() if third_cut else None
    # first_cut_time = datetime.time(18, 1) if third_cut else None

    # Ma'lumotlar allaqachon yuklab olinmagan bo'lsa
    if not (dt_k.exists() and cr_k.exists()):
        # Get and save remainders by banks
        remainders = korschot.get_branch_remainders()

        if remainders is not None:
            remainders.to_csv(DOWNLOADS_FOLDER / f'Remainders_{oper_date}.csv')
            runners = (Runner(korschot, 0), Runner(korschot, 1))

            print("Operatsiyalarni parallel yuklab olish boshlanmoqda...")
            with ProcessPoolExecutor() as executor:
                results = executor.map(get_by_branch, runners)

            for result in results:
                site, df = result
                df['SUMMA'] = df['SUMMA'].str.replace(' ', '').astype('float')
                df.drop('CLIENT_' + site.side, axis=1, inplace=True)

                df['PURPOSE_TEXT'] = where(df['SUMMA']>=PURPOSE_BOUNDARY, df['PURPOSE_TEXT'], '')
                df.replace('=', ' ', regex=True, inplace=True)
                df.to_csv(DOWNLOADS_FOLDER / f'Liquidity_{oper_date}_{site.side}.csv', index=False)
        else:
            print("Ochiq balans bo'lgan kunni tanlang!")
            exit()
            
        seconds = time.time() - start_time
        print('Time taken:', time.strftime("%H:%M:%S", time.gmtime(seconds)), '\n') 

    analyse(oper_date, prelimenary, gsb, ocb, without_anor, first_cut, third_cut, first_cut_time)
    