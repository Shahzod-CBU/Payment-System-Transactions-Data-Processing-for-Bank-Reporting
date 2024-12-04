import win32com.client as win32
from pywintypes import com_error
from pathlib import Path
import pandas as pd
import sys
from collections import defaultdict
from ast import literal_eval
from get_data import DOWNLOADS_FOLDER

# written by Shahzod on 4 Dec 2023
# credits to Trenton McKinney "How to Create a Pivot Table in Excel with the Python win32com Module"

'''Faktorlarni jamlovchi "pivot table"larni Excelda yasash'''

win32c = win32.constants
PATH = ''


def create_pivot_table(wb: object, ws1: object, pt_ws: object, ws_name: str, pt_name: str, 
    pt_rows: list, pt_cols: list, pt_filters: list, pt_fields: list, dataframe):
    """
    wb = workbook1 reference
    ws1 = worksheet1
    pt_ws = pivot table worksheet number
    ws_name = pivot table worksheet name
    pt_name = name given to pivot table
    pt_rows, pt_cols, pt_filters, pt_fields: values selected for filling the pivot tables
    """
    side = ws_name[-2:]
    # pivot table location
    pt_loc = len(pt_filters) + 2
    
    # grab the pivot table source data
    pc = wb.PivotCaches().Create(SourceType=win32c.xlDatabase, 
        # SourceData=(ws1.UsedRange if clients else pt_name), 
        SourceData=pt_name, 
        Version=win32c.xlPivotTableVersion12)
    
    # create the pivot table object
    pc.CreatePivotTable(TableDestination=f'{ws_name}!R{pt_loc}C1', TableName=pt_name, DefaultVersion=win32c.xlPivotTableVersion12)

    # selecte the pivot table work sheet and location to create the pivot table
    pt_ws.Select()
    pt_ws.Cells(pt_loc, 1).Select()
    pt_obj = pt_ws.PivotTables(pt_name)
    pt_obj.FieldListSortAscending = True
    
    # Sets the rows, columns and filters of the pivot table
    for field_list, field_r in ((pt_filters, win32c.xlPageField), (pt_rows, win32c.xlRowField), (pt_cols, win32c.xlColumnField)):
        for i, value in enumerate(field_list):
            pt_obj.PivotFields(value).Orientation = field_r
            pt_obj.PivotFields(value).Position = i + 1

    # Sets the Values of the pivot table
    for field in pt_fields:
        pt_obj.AddDataField(pt_obj.PivotFields(field[0]), field[1], field[2]).NumberFormat = field[3]

    # Visiblity True or Valse
    pt_obj.ShowValuesRow = True
    pt_obj.ColumnGrand = True
    
    pt_obj.PivotFields('Factor2').PivotItems('Клиринг').Visible = False
    try:
        pt_obj.PivotFields('DT').PivotItems('27402').Visible = False
    except:
        print('Cannot make 27402 invisible')

    pt_obj.CompactLayoutRowHeader = "Factors"

    # EKS kirim-chiqimini aniqlash 
    pt_obj.PivotFields(side).Orientation = win32c.xlRowField
    pt_obj.PivotFields(side).Position = 2

    try:
        EKS = float(pt_obj.GetPivotData("SUMMA_" + side, side, "23402", "Factor2","Ҳукумат").__str__())
    except:
        EKS = 0
        print('NO EKS')
    
    if side == 'DT':
        pt_obj.PivotFields(side).Orientation = win32c.xlPageField
        pt_obj.PivotFields(side).Position = 1
    else:
        pt_obj.PivotFields(side).Orientation = win32c.xlHidden

    try:
        birja = float(pt_obj.GetPivotData("SUMMA_" + side, "Factor2","Биржа билан операциялар").__str__())
        dataframe['VALUTA_' + side] = '{:,.0f}'.format(birja/10**9).replace(',', ' ')
    except:
        print('"Биржа билан операциялар" мавжуд эмас')

    pt_obj.PivotFields('Factor1').Orientation = win32c.xlRowField
    pt_obj.PivotFields('Factor1').Position = 2

    # Sortirovka
    pt_obj.PivotFields('Factor2').AutoSort(Order=win32c.xlDescending, Field='Total by SUMMA')
    pt_obj.PivotFields('Factor1').AutoSort(Order=win32c.xlDescending, Field='Total by SUMMA')
    pt_obj.PivotFields('Factor1').PivotFilters.Add2(Type=win32c.xlValueIsGreaterThanOrEqualTo, 
        DataField=pt_obj.PivotFields("Total by SUMMA"), Value1=8.5*10**9)
    # pt_obj.PivotFields('Factor1').PivotFilters.Add2(Type=win32c.xlValueIsGreaterThanOrEqualTo, 
    #     DataField=pt_obj.PivotFields("Total by SUMMA"), Value1=10**6)

    dataframe[ws_name] = literal_eval(str(pt_obj.TableRange2))

    pt_obj.PivotFields('Factor2').Orientation = win32c.xlPageField
    pt_obj.PivotFields('Factor2').Position = 1

    # Factor2'da faqat Hukumatni qoldirish
    pt_obj.PivotFields("Factor2").EnableMultiplePageItems = True
    len_factor1 = pt_obj.PivotFields("Factor2").PivotItems().Count
    for m in range(1, len_factor1+1):
        pt_item = pt_obj.PivotFields("Factor2").PivotItems(m)
        if pt_item.Name != "Ҳукумат":
            pt_item.Visible = False

    # # VAQTINCHALIK
    # pt_obj.PivotFields('BANK').Orientation = win32c.xlHidden
        
    # return literal_eval(str(pt_obj.TableRange1))

    pt_obj.PivotFields('BANK').Orientation = win32c.xlRowField
    pt_obj.PivotFields('BANK').Position = 1


    # MUNIS kliring: 
    #   1) Dt 19997000900009001001  Cr 23402000300100001010 (Kun davomida bir necha marta, Operbankdan ko'rish mumkin)
    #   2) Dt 27480000100009001001  Cr 29896000400009001001 ("Центр расчетов"da kuniga 2 marta - Korschotga ta'sir qiluvchi operatsiya)
    #   3) Dt 29896000400009001001  Cr 19997000900009001001 (09001da shunday bo'lishi kerak)

    if side == 'CR':
        try:
            EKS += float(pt_obj.GetPivotData("SUMMA_CR", "Factor1","МУНИС тизими орқали тўловлар","BANK","999-Марказий банк").__str__())
        except:
            print('NO MUNIS')

    dataframe['EKS_' + side] = '{:,.0f}'.format(EKS/10**9).replace(',', ' ')


def run_excel(filename: str):
# def run_excel(filename: str, oper_date):
    # create excel object
    excel = win32.gencache.EnsureDispatch('Excel.Application')

    # excel can be visible or not
    excel.Interactive = False
    # excel.Visible = False
    excel.ScreenUpdating = False
    excel.DisplayAlerts = False
    excel.EnableEvents = False
    excel.Calculation = win32c.xlCalculationManual
        
    # try except for file / path
    try:
        print('Opening the Liquidity factor workbook...')
        wb = excel.Workbooks.Open(filename)
    except com_error as e:
        if e.excepinfo[5] == -2146827284:
            print(f'Failed to open spreadsheet.  Invalid filename or location: {filename}')
        else:
            raise e
        sys.exit(1)

    dfs = defaultdict(dict)
    tur_provod = ['CR', 'DT']

    for n, side in enumerate(tur_provod):
        reverse_side = tur_provod[1-n]

        # set worksheet
        ws1 = wb.Sheets(side)
        
        # Setup and call pivot_table
        ws2_name = 'Pivot' + side
        summa_name = 'SUMMA_' + side
        pt_name = ws2_name + 'Table' # must be a string

        ws1.ListObjects.Add(win32c.xlSrcRange, ws1.UsedRange).Name =pt_name
        # ws1.ListObjects(ws2_name).ShowTableStyleRowStripes = False
        ws1.ListObjects(pt_name).TableStyle = "TableStyleMedium2"

        wb.Queries.Add(Name=ws2_name, Formula=r"""
        let
                Source = Excel.CurrentWorkbook(){[Name="JadvalNomi"]}[Content],
                #"Changed Type" = Table.TransformColumnTypes(Source,{{"BANK_DT", type text}, {"ACCOUNT_DT", type text}, {"BANK_CR", type text}, {"ACCOUNT_CR", type text}, {"SUMMA", type number}, {"CLIENT", type text}, {"PURPOSE", type text}, {"PLATFORM", type text},  {"DT", type text}, {"CR", type text}, {"Factor1", type text}, {"Factor2", type text}, {"BANK", type text}, {"PURPOSE_TEXT", type text}})
        in
                #"Changed Type"
        """.replace("JadvalNomi", pt_name).replace("SUMMA", summa_name).replace("CLIENT", "CLIENT_"+reverse_side), )
        
        wb.Connections.Add2('Query - ' + ws2_name, 'Connection',
            "OLEDB;Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;Location=" + pt_name,
            f'SELECT * FROM [{pt_name}]', 2)

        wb.Sheets.Add().Name = ws2_name
        ws2 = wb.Sheets(ws2_name)
        
        # pt_rows = ['Factor2', 'Factor1']  # must be a list
        pt_rows = ['Factor2']  # must be a list
        pt_cols = ['BANK']  # must be a list
        pt_filters = ['DT']  # must be a list
        # [0]: field name [1]: pivot table column name [3]: calulation method [4]: number format
        pt_fields = [ [summa_name, 'Total by SUMMA', win32c.xlSum, '# ##0'] ]
        
        print('Creating a pivot table for', side)
        create_pivot_table(wb, ws1, ws2, ws2_name, pt_name, pt_rows, pt_cols, pt_filters, pt_fields, dataframe=dfs)
        # tab = create_pivot_table(wb, ws1, ws2, ws2_name, pt_name, pt_rows, pt_cols, pt_filters, pt_fields, dataframe=dfs)
        # df = pd.DataFrame(list(tab[1:-1]))
        # df.to_csv(f'{side}_{oper_date}.csv', header=False, index=False)


    print('Creating the Net pivot table')
    wb.Queries.Add(Name="TotalPivot", Formula=r"""
    let
            Source = Table.Combine({PivotDT, PivotCR})
    in
            Source
    """)
        
    wb.Connections.Add2('Query - TotalPivot', 'Connection',
        "OLEDB;Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;Location=TotalPivot",
        f'SELECT * FROM [TotalPivot]', 2)

    pivot_sheet = wb.Sheets.Add()
    wb.ActiveSheet.Name = "PivotNet"

    pivot_caches = wb.PivotCaches()
    pivot_cache = pivot_caches.Create(
            SourceType=win32c.xlExternal,
            SourceData=wb.Connections('Query - TotalPivot'),
            Version=win32c.xlPivotTableVersion12
    )

    # Create the pivot table
    pivot_tables = pivot_sheet.PivotTables()
    pivot_table = pivot_tables.Add(pivot_cache, pivot_sheet.Cells(2, 1), "PivotNet", DefaultVersion=win32c.xlPivotTableVersion12)

    ## pivot_table = wb.Sheets("PivotNet").PivotTables("PivotNet")
    # Add fields
    pivot_table.CalculatedFields().Add("Еffect", "=(SUMMA_DT-SUMMA_CR)/10^9").Orientation = win32c.xlDataField
    # pivot_table.PivotFields('Сумма по полю Еffect').Caption = 'Net effect'
    pivot_table.DataBodyRange.NumberFormat = "# ##0"
    factor2 = pivot_table.PivotFields('Factor2')
    factor2.Orientation = win32c.xlRowField
    factor2.Position = 1
    factor2.AutoSort(Order=win32c.xlDescending, Field='Сумма по полю Еffect')
    factor2.PivotItems('Клиринг').Visible = False
    pivot_table.PivotFields('DT').Orientation = win32c.xlPageField
    try:
        pivot_table.PivotFields('DT').PivotItems('27402').Visible = False
    except:
        pass

    effect_field = pivot_table.PivotFields('Сумма по полю Еffect')
    effect_field.DataRange.FormatConditions.Add(Type=win32c.xlCellValue, Operator=win32c.xlEqual, Formula1="=0")
    cond_format = effect_field.DataRange.FormatConditions.Item(1)
    cond_format.Font.ThemeColor = win32c.xlThemeColorDark1
    cond_format.Font.TintAndShade = 0
    cond_format.StopIfTrue = False
    cond_format.ScopeType = win32c.xlDataFieldScope

    pivot_table.PivotFields('BANK').Orientation = win32c.xlColumnField
    pivot_table.CompactLayoutRowHeader = "Factors"
    dfs['Net'] = literal_eval(str(pivot_table.TableRange2))

    excel.DisplayAlerts = True
    excel.Interactive = True
    excel.EnableEvents = True
    excel.Calculation = win32c.xlCalculationAutomatic

    wb.Save()
    # wb.Close()

    return dfs
    
#     wb.Close(True)
#     excel.Quit()

if __name__ == '__main__':
    # FOR TESTING
    oper_date = '13.12.2023'

    # f_path = Path.cwd()  # file in current working directory
    f_path = Path('C:\\Users\\msd13\\Documents\\Test Liquidity\\кунлик корр. счет 2023\\12 Декабрь\\Декабрь')
    f_name =  f'Liquidity factor {oper_date}.xlsx'
    
    # function call
    dfs = run_excel(f_path / f_name)

