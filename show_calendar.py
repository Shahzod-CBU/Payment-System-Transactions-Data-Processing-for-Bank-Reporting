from tkinter import Tk, Button, Checkbutton, IntVar
from tkcalendar import Calendar
from datetime import datetime

# created by Shahzod on 9 Dec 2023

'''Sana tanlash uchun kalendarni yaratish'''


def grad_date():
    global cal, root, prelimenary, gsb, ocb, without_anor
    oper_date = cal.get_date()
    root.quit()
    return oper_date, prelimenary.get(), gsb.get(), ocb.get(), without_anor.get(),  first_cut.get(), third_cut.get()


def create_calendar(is_liquidity=False):
    global cal, root, prelimenary, gsb, ocb, without_anor, first_cut, third_cut

    # Create Object
    root = Tk()
    prelimenary = IntVar()
    gsb = IntVar()
    ocb = IntVar()
    without_anor = IntVar()
    first_cut = IntVar()
    third_cut = IntVar()

    # Set geometry
    h = 375 if is_liquidity else 280
    root.geometry(f"300x{h}")
    root.eval('tk::PlaceWindow . center')
    root.title('Sana tanlang')

    td = datetime.today()
    # Add Calendar
    cal = Calendar(root, selectmode = 'day', year = td.year, month = td.month,
                day = td.day, date_pattern='DD.MM.YYYY')

    cal.pack(pady = 15)

    # Add Button and Label
    Button(root, text = "  OK  ", command = grad_date).pack(pady = 5)

    if is_liquidity:
        c = Checkbutton(root, text="First cut", variable=first_cut)
        c.place(x=40, y=265)
        c1 = Checkbutton(root, text="Third cut", variable=third_cut)
        c1.place(x=150, y=265)

        c2 = Checkbutton(root, text="DQQ chiqarildi", variable=gsb)
        c2.place(x=40, y=300)
        c3 = Checkbutton(root, text="MBO chiqarildi", variable=ocb)
        c3.place(x=150, y=300)
        c4 = Checkbutton(root, text="Dastlabki", variable=prelimenary)
        c4.place(x=40, y=335)
        c5 = Checkbutton(root, text="ANORsiz", variable=without_anor)
        c5.place(x=150, y=335)


    # Execute Tkinter
    root.mainloop()
    
    return root