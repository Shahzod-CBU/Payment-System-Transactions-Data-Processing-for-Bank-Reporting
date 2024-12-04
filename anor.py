from get_data import *
from show_calendar import create_calendar, grad_date

# written by Shahzod on 2 Dec 2023

'''ANORdan operatsiyalarni yuklab olish'''

if __name__ == '__main__':
    start_time = time.time()
    # CPU yadrolari sonini aniqlash
    WORKERS = max(os.cpu_count()-1, 1)

    # oper_date = '13.12.2023'

    # Get the operation date with the Calendar picker
    root = create_calendar()
    oper_date = grad_date()[0]
    print(oper_date)

    try:
        root.destroy()
    except:
        # Chiqish tugmasi bosilganda
        exit()

    anor = Site(1, oper_date)

    print("ANORga kirish...")
    anor.get_cookie()

    print('Kliring sessiyalarini aniqlash...')
    session_details = anor.get_session_details()

    if len(session_details) == 0:
        print("Ochiq balans bo'lgan kunni tanlang!")
        exit()

    # print('session_ids', session_details)

    anors = []
    for n, (session_id, session_dates) in enumerate(session_details.items(), start=1):
        # in order not to lose information about site cookie, we create a copy instance of class
        anor = deepcopy(anor)
        anor.PARAMS['session'] = session_id
        anor.PARAMS['data'] = session_dates[0]
        anor.PARAMS['data_end'] = session_dates[1]
        anor.clearing = f'Session {n}'
        anors.append(anor)

    runners = []
    for anor in anors:
        runners.append(Runner(anor, 0))
        runners.append(Runner(anor, 1))

    print("Operatsiyalarni parallel yuklab olish boshlanmoqda...")
    processes = min(WORKERS, len(runners))
    with ProcessPoolExecutor(max_workers=processes) as executor:
        results = executor.map(get_by_branch, runners)

    # ANOR oborotlari yig'indisi va korschotdagi kliring summalari bir xilligiga ishonch hosil 
    # qilish uchun kliring sessiyalari bo'yicha yig'indilarni check_totals'ga saqlab qo'yamiz
    check_totals = defaultdict(dict)

    dfs = {'DT': [], 'CR': []}
    for result in results:
        site, df = result
        df['SUMMA'] = df['SUMMA'].str.replace(',', '').astype('float')
        check_totals[site.PARAMS['session']][site.side] = round(df['SUMMA'].sum(), 2)
        dfs[site.side].append(df)

    for side, dfs_list in dfs.items():
        merged_df = pd.concat(dfs_list, ignore_index=True)
        merged_df.drop('CLIENT_' + side, axis=1, inplace=True)

        merged_df['PURPOSE_TEXT'] = where(merged_df['SUMMA']>=PURPOSE_BOUNDARY, merged_df['PURPOSE_TEXT'], '')

        # Excelda ochganda "=" tufayli xato yuzaga kelmasligi uchun
        merged_df.replace('=', ' ', regex=True, inplace=True)
        merged_df.to_csv(DOWNLOADS_FOLDER / f'ANOR_{oper_date}_{side}.csv', index=False)
    
    with open(DOWNLOADS_FOLDER / f'Total_{oper_date}.json', 'w') as outfile:
        json.dump(check_totals, outfile)

    seconds = time.time() - start_time
    print('Time taken:', time.strftime("%H:%M:%S", time.gmtime(seconds)), '\n') 