# C:\Users\juanf\Downloads\sweetsol\ss_backend\venv\Scripts


from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from enum import Enum
from pydantic import BaseModel

import psycopg as pg
from datetime import date, timedelta
import calendar


CONN_ARGS = {
    "host": "_HOST_HERE_",
    "port": "_PORT_HERE_",
    "dbname": "_DBNAME_HERE_",
    "user": "_USER_HERE_",
    "password": "_PASSWORD_HERE_"
}

MONTHS = [
    "Ene",
    "Feb",
    "Mar",
    "Abr",
    "May",
    "Jun",
    "Jul",
    "Ago",
    "Sep",
    "Oct",
    "Nov",
    "Dic"
]

WEEKDAYS = [
    "Lun",
    "Mar",
    "Mie",
    "Jue",
    "Vie",
    "Sab",
    "Dom"
]


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins = [ "*" ],
    allow_methods = [ "*" ],
    allow_headers = [ "*" ]
)


@app.get( "/agents" )
def agents():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select * from main.agent
            """)

            return cur.fetchall()


class Client( BaseModel ):
    name: str
    lastname: str
    email: str
    cty_code: str
    phone_num: str

@app.post( "/clients" )
def post_clients( client: Client ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                insert into main.client(
                    name,
                    lastname,
                    email,
                    cty_code,
                    phone_num
                ) values(
                    '{ client.name }',
                    '{ client.lastname }',
                    '{ client.email }',
                    '{ client.cty_code }',
                    '{ client.phone_num }'
                )
            """)


@app.post( "/clients/{client_vid}/deposit" )
def post_client_deposit(
    client_vid: int,
    deposit_date: date
):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select count(*)
                from main.client_milestone cm
                where cm.client_vid = { client_vid }
                and cm.milestone_type = 'DEPOSIT'
            """)

            if( cur.fetchone()[ 0 ] ):
                raise HTTPException(
                    status_code = 409,
                    detail = "Client already has a deposit date"
                )

            cur.execute(f"""
                insert into main.client_milestone(
                    client_vid,
                    milestone_type,
                    date
                ) values(
                    { client_vid },
                    'DEPOSIT',
                    '{ deposit_date }'
                )
            """)


@app.post( "/clients/{client_vid}/arrival" )
def post_client_arrival(
    client_vid: int,
    arrival_date: date
):
    CONTACTS = [ 3, 7, 15, 20, 30, 50, 70, 90, 110, 150, 200 ]

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select count(*)
                from main.client_milestone cm
                where cm.client_vid = { client_vid }
                and cm.milestone_type = 'ARRIVAL'
            """)

            if( cur.fetchone()[ 0 ] ):
                raise HTTPException(
                    status_code = 409,
                    detail = "Client already has an arrival date"
                )
            
            cur.execute(f"""
                select to_char(cm.date, 'YYYY-MM-DD')
                from main.client_milestone cm
                where cm.client_vid = { client_vid }
                and cm.milestone_type = 'DEPOSIT'
            """)

            deposit_date = cur.fetchone()

            if( not deposit_date ):
                raise HTTPException(
                    status_code = 409,
                    detail = "Client does not have a deposit date"
                )

            deposit_date = date.fromisoformat( deposit_date[ 0 ] )
            timespan = arrival_date - deposit_date

            risk_level = ""
            if( timespan.days <= 60 ):
                risk_level = "L_RISK"
            elif( timespan.days <= 120 ):
                risk_level = "M_RISK"
            else:
                risk_level = "H_RISK"

            interactions = ""
            i = 0
            cont_date = arrival_date - timedelta( days = CONTACTS[ i ] )
            while( cont_date > deposit_date ):
                interactions += f"""
                (
                    { client_vid },
                    '{ cont_date.strftime( "%Y-%m-%d" ) }',
                    'Contacto { CONTACTS[ i ] } antes de llegada...',
                    false
                ), """

                i += 1
                cont_date = arrival_date - timedelta( days = CONTACTS[ i ] )

            if( len( interactions ) ):
                interactions = interactions[ : -2 ]

            cur.execute(f"""
                insert into main.client_milestone(
                    client_vid,
                    milestone_type,
                    date
                ) values(
                    { client_vid },
                    'ARRIVAL',
                    '{ arrival_date.strftime( "%Y-%m-%d" ) }'
                )
            """)

            cur.execute(f"""
                insert into main.client_risk_level(
                    client_vid,
                    risk_level
                ) values(
                    { client_vid },
                    '{ risk_level }'
                )
            """)

            if( len( interactions ) ):
                cur.execute(f"""
                    insert into main.interaction(
                        client_vid,
                        inter_date,
                        inter_desc,
                        checked
                    ) values { interactions }
                """)


@app.get( "/clients/milestones" )
def clients_milestones(
    exc_cont: bool = False,
    exc_dep: bool = False,
    exc_arr: bool = False 
):
    filters = []
    if( not exc_cont ): filters.append( "CONTACT" )
    if( not exc_dep ): filters.append( "DEPOSIT" )
    if( not exc_arr ): filters.append( "ARRIVAL" )

    filters = "(" + "".join( list( map( lambda e: f"'{ e }', ", filters ) ) )[ : -2 ] + ")"

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select c.name, cm.milestone_type, cm.date
                from main.client c, main.client_milestone cm
                where c.vid = cm.client_vid
                and cm.milestone_type in { filters }
                order by cm.date
            """)
        
            return cur.fetchall()
        

def set_limits_for_year( year ):
    return(
        date( year, 1, 1 ),
        date( year, 12, 31 )
    )

def set_limits_for_month( year, month ):
    return(
        date( year, month, 1 ),
        date( year, month, calendar.monthrange( year, month )[ 1 ] )
    )

def set_limits_for_week( year, month, week ):
    mc = calendar.monthcalendar( year, month )

    if( week == 0 and not mc[ 0 ][ 0 ] ):
        if( month == 1 ):
            prev_month = 12
            prev_year = year - 1
        else:
            prev_month = month - 1
            prev_year = year
        low = date( prev_year, prev_month, calendar.monthcalendar( prev_year, prev_month )[ -1 ][ 0 ] )
    else:
        low = date( year, month, mc[ week ][ 0 ] )

    high = low + timedelta( days = 6 )
    
    return low, high

def metrics_by_month( year, dchecks, dcounts ):
    risk_levels = { rl: 0 for rl in [ "L_RISK", "M_RISK", "H_RISK" ] }
    g_checks = risk_levels.copy()
    g_counts = risk_levels.copy()
    checks = { date( year, i, 1 ).isoformat(): risk_levels.copy() for i in range( 1, 13 ) }
    counts = { date( year, i, 1 ).isoformat(): risk_levels.copy() for i in range( 1, 13 ) }

    for row in dchecks:
        g_checks[ row[ 1 ] ] += row[ 2 ]
        checks[ row[ 0 ] ][ row[ 1 ] ] = row[ 2 ]
    
    for row in dcounts:
        g_counts[ row[ 1 ] ] += row[ 2 ]
        counts[ row[ 0 ] ][ row[ 1 ] ] = row[ 2 ]
    
    for rl in risk_levels.keys():
            if( g_counts[ rl ] ):
                g_checks[ rl ] = 100 * g_checks[ rl ] / g_counts[ rl ]
            else:
                g_checks[ rl ] = 100
            g_checks[ rl ] = "{:.0f}".format( g_checks[ rl ] )

    for month in checks.keys():
        for rl in risk_levels.keys():
            if( counts[ month ][ rl ] ):
                checks[ month ][ rl ] = 100 * checks[ month ][ rl ] / counts[ month ][ rl ]
            else:
                checks[ month ][ rl ] = 100
            checks[ month ][ rl ] = "{:.0f}".format( checks[ month ][ rl ] )
    
    return {
        "global": { "counts": g_counts, "progress": g_checks },
        "segments": {
            "labels": MONTHS,
            "counts": [ counts[ month ] for month in sorted( counts.keys() ) ],
            "progress": [ checks[ month ] for month in sorted( checks.keys() ) ]
        }
    }

def metrics_by_week( year, month, dchecks, dcounts ):
    risk_levels = { rl: 0 for rl in [ "L_RISK", "M_RISK", "H_RISK" ] }
    g_checks = risk_levels.copy()
    g_counts = risk_levels.copy()
    checks = {}
    counts = {}

    mc = calendar.monthcalendar( year, month )

    i = 0

    if( not mc[ 0 ][ 0 ] ):
        if( month == 1 ):
            prev_month = 12
            prev_year = year - 1
        else:
            prev_month = month - 1
            prev_year = year

        week_date = date(
            prev_year,
            prev_month,
            calendar.monthcalendar( prev_year, prev_month )[ -1 ][ 0 ]
        ).strftime( "%Y-%m-%d" )

        checks[ week_date ] = risk_levels.copy()
        counts[ week_date ] = risk_levels.copy()
        
        i += 1

    for i in range( i, len( mc ) ):
        week_date = date( year, month, mc[ i ][ 0 ] ).strftime( "%Y-%m-%d" )
        checks[ week_date ] = risk_levels.copy()
        counts[ week_date ] = risk_levels.copy()

    for row in dchecks:
        g_checks[ row[ 1 ] ] += row[ 2 ]
        checks[ row[ 0 ] ][ row[ 1 ] ] = row[ 2 ]
    
    for row in dcounts:
        g_counts[ row[ 1 ] ] += row[ 2 ]
        counts[ row[ 0 ] ][ row[ 1 ] ] = row[ 2 ]
    
    for rl in risk_levels.keys():
            if( g_counts[ rl ] ):
                g_checks[ rl ] = 100 * g_checks[ rl ] / g_counts[ rl ]
            else:
                g_checks[ rl ] = 100
            g_checks[ rl ] = "{:.0f}".format( g_checks[ rl ] )

    for week in checks.keys():
        for rl in risk_levels.keys():
            if( counts[ week ][ rl ] ):
                checks[ week ][ rl ] = 100 * checks[ week ][ rl ] / counts[ week ][ rl ]
            else:
                checks[ week ][ rl ] = 100
            checks[ week ][ rl ] = "{:.0f}".format( checks[ week ][ rl ] )
    
    return {
        "global": { "counts": g_counts, "progress": g_checks },
        "segments": {
            "labels": [ f"S{ i }" for i in range( 1, len( counts.keys() ) + 1 ) ],
            "counts": [ counts[ week ] for week in sorted( counts.keys() ) ],
            "progress": [ checks[ week ] for week in sorted( checks.keys() ) ]
        }
    }

def metrics_by_weekday( year, month, week, dchecks, dcounts ):
    risk_levels = { rl: 0 for rl in [ "L_RISK", "M_RISK", "H_RISK" ] }
    g_checks = risk_levels.copy()
    g_counts = risk_levels.copy()
    checks = {}
    counts = {}

    l, _ = set_limits_for_week( year, month, week )
    for i in range( 0, 7 ):
        checks[ ( l + timedelta( days = i * 1 ) ).strftime( "%Y-%m-%d" ) ] = risk_levels.copy()
        counts[ ( l + timedelta( days = i * 1 ) ).strftime( "%Y-%m-%d" ) ] = risk_levels.copy()

    for row in dchecks:
        g_checks[ row[ 1 ] ] += row[ 2 ]
        checks[ row[ 0 ] ][ row[ 1 ] ] = row[ 2 ]
    
    for row in dcounts:
        g_counts[ row[ 1 ] ] += row[ 2 ]
        counts[ row[ 0 ] ][ row[ 1 ] ] = row[ 2 ]
    
    for rl in risk_levels.keys():
            if( g_counts[ rl ] ):
                g_checks[ rl ] = 100 * g_checks[ rl ] / g_counts[ rl ]
            else:
                g_checks[ rl ] = 100
            g_checks[ rl ] = "{:.0f}".format( g_checks[ rl ] )

    for weekday in checks.keys():
        for rl in risk_levels.keys():
            if( counts[ weekday ][ rl ] ):
                checks[ weekday ][ rl ] = 100 * checks[ weekday ][ rl ] / counts[ weekday ][ rl ]
            else:
                checks[ weekday ][ rl ] = 100
            checks[ weekday ][ rl ] = "{:.0f}".format( checks[ weekday ][ rl ] )
    
    return {
        "global": { "counts": g_counts, "progress": g_checks },
        "segments": {
            "labels": WEEKDAYS,
            "counts": [ counts[ weekday ] for weekday in sorted( counts.keys() ) ],
            "progress": [ checks[ weekday ] for weekday in sorted( checks.keys() ) ]
        }
    }

@app.get( "/metrics" )
def interactions_count(
    year: int,
    month: int = None,
    week: int = None,

    # exc_lr: bool = False,
    # exc_mr: bool = False,
    # exc_hr: bool = False,
):
    if( month == None and week != None ):
        raise HTTPException( status_code = 422 )
    if( month != None and ( month < 1 or month > 12 ) ):
        raise HTTPException( status_code = 422 )
    
    if( month and week != None ):
        l, h = set_limits_for_week( year, month, week )
        trunc = "day"
    elif( month ):
        l, h = set_limits_for_month( year, month )
        trunc = "week"
    else:
        trunc = "month"
        l, h = set_limits_for_year( year )

    # filters = []
    # if( not exc_lr ): filters.append( "L_RISK" )
    # if( not exc_mr ): filters.append( "M_RISK" )
    # if( not exc_hr ): filters.append( "H_RISK" )

    # if( not len( filters ) ):
    #     return { "labels": [], "datasets": [] }

    # filters_str = "(" + "".join( list( map( lambda e: f"'{ e }', ", filters ) ) )[ : -2 ] + ")"

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select to_char(date_trunc('{ trunc }', i.inter_date), 'YYYY-MM-DD') as { trunc }, crl.risk_level, count(*)
                from main.interaction i, main.client c, main.client_risk_level crl
                where i.client_vid = c.vid
                and c.vid = crl.client_vid
                and i.checked = true
                and i.inter_date >= '{ l }'
                and i.inter_date <= '{ h }'
                group by { trunc }, crl.risk_level
                order by { trunc }
            """)

            checks = cur.fetchall()

            cur.execute(f"""
                select to_char(date_trunc('{ trunc }', i.inter_date), 'YYYY-MM-DD') as { trunc }, crl.risk_level, count(*)
                from main.interaction i, main.client c, main.client_risk_level crl
                where i.client_vid = c.vid
                and c.vid = crl.client_vid
                and i.inter_date >= '{ l }'
                and i.inter_date <= '{ h }'
                group by { trunc }, crl.risk_level
                order by { trunc }
            """)

            counts = cur.fetchall()

            if( month != None and week != None ):
                return metrics_by_weekday( year, month, week, checks, counts )
            elif( month != None ):
                return metrics_by_week( year, month, checks, counts )
            else:
                return metrics_by_month( year, checks, counts )


@app.get( "/interactions" )
def interactions(
    year: int,
    month: int = None,
    week: int = None,
    day: int = None,

    exc_lr: bool = False,
    exc_mr: bool = False,
    exc_hr: bool = False,
):
    if( month == None and week != None ):
        raise HTTPException( status_code = 422 )
    if( ( month == None or week == None ) and day != None ):
        raise HTTPException( status_code = 422 )
    if( month != None and ( month < 1 or month > 12 ) ):
        raise HTTPException( status_code = 422 )
    
    if( month and week != None and day != None ):
        mc = calendar.monthcalendar( year, month )

        if( mc[ week ][ day ] == 0 ):
            if( week == 0 ):
                if( month == 1 ):
                    prev_month = 12
                    prev_year = year - 1
                else:
                    prev_month = month - 1
                    prev_year = year        
                
                l = date(
                    prev_year,
                    prev_month,
                    calendar.monthcalendar( prev_year, prev_month )[ -1 ][ day ]
                )

            else:
                if( month == 12 ):
                    next_month = 1
                    next_year = year + 1
                else:
                    next_month = month + 1
                    next_year = year

                l = date(
                    next_year,
                    next_month,
                    calendar.monthcalendar( next_year, next_month )[ 0 ][ day ]
                )
        
        else:
            l = date( year, month, mc[ week ][ day ] )
        
        h = l

    elif( month and week != None ):
        l, h = set_limits_for_week( year, month, week )

    elif( month ):
        l, h = set_limits_for_month( year, month )

    else:
        l, h = set_limits_for_year( year )
    
    span = f"""
        and i.inter_date >= '{ l }'
        and i.inter_date <= '{ h }'
    """

    filters = []
    if( not exc_lr ): filters.append( "L_RISK" )
    if( not exc_mr ): filters.append( "M_RISK" )
    if( not exc_hr ): filters.append( "H_RISK" )

    if( not len( filters ) ):
        return []

    filters_str = "(" + "".join( list( map( lambda e: f"'{ e }', ", filters ) ) )[ : -2 ] + ")"

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            # print(f"""
            #     select c.vid, c.name, c.lastname, c.email, c.cty_code, c.phone_num, crl.risk_level, to_char(i.inter_date, 'YYYY-MM-DD'), i.checked
            #     from main.client c, main.client_risk_level crl, main.interaction i
            #     where c.vid = crl.client_vid
            #     and c.vid = i.client_vid
            #     and crl.risk_level in { filters_str }
            #     { span }
            # """)

            cur.execute(f"""
                select c.vid, c.name, c.lastname, c.email, c.cty_code, c.phone_num, crl.risk_level, to_char(i.inter_date, 'YYYY-MM-DD'), i.checked
                from main.client c, main.client_risk_level crl, main.interaction i
                where c.vid = crl.client_vid
                and c.vid = i.client_vid
                and crl.risk_level in { filters_str }
                { span }
            """)

            ret = [{
                "client": {
                    "vid": row[ 0 ],
                    "name": row[ 1 ],
                    "lastname": row[ 2 ],
                    "email": row[ 3 ],
                    "cty_code": row[ 4 ],
                    "phone_num": row[ 5 ],
                    "risk_level": row[ 6 ]
                },
                "interaction": {
                    "inter_date": row[ 7 ],
                    "checked": row[ 8 ]
                }
            } for row in cur.fetchall() ]

            return ret


class Interaction( BaseModel ):
    client_vid: int
    inter_date: str
    inter_desc: str

@app.post( "/interactions/create" )
def create_interaction( interaction: Interaction ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                insert into main.interaction(client_vid, inter_date, inter_desc, checked) values(
                    { interaction.client_vid },
                    '{ interaction.inter_date }',
                    '{ interaction.inter_desc }',
                    false
                )
            """)


@app.post( "/interactions/checked/toogle" )
def interactions_checked_toogle(
    client_vid: int,
    inter_date: str
):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                update main.interaction
                set checked = not checked
                where interaction.client_vid = { client_vid }
                and interaction.inter_date = '{ inter_date }'
                returning checked
            """)

            return { "new_value": cur.fetchone()[ 0 ] }


@app.get( "/client/{client_vid}/roadmap" )
def client_roadmap( client_vid: int ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            datasets = { "CONTACT": [], "DEPOSIT": [], "ARRIVAL": [] }

            cur.execute(f"""
                select to_char(i.inter_date, 'YYYY-MM-DD'), i.inter_desc, i.checked
                from main.client c, main.interaction i
                where i.client_vid = c.vid
                and i.client_vid = { client_vid }
                order by i.inter_date
            """)

            interactions = cur.fetchall()

            cur.execute(f"""
                select to_char(cm.date, 'YYYY-MM-DD')
                from main.client_milestone cm
                where cm.client_vid = { client_vid }
                order by cm.date
            """)

            milestones = cur.fetchall()
            dep_date = milestones[ 0 ][ 0 ]
            arr_date = milestones[ 1 ][ 0 ]

            datasets = {
                "INTERACTIONS": [],
                "MILESTONES": [ { "x": dep_date, "y": 1 } ]
            }

            labels = [ dep_date ]

            for inter in interactions:
                labels.append( inter[ 0 ] )
                datasets[ "INTERACTIONS" ].append({
                    "x": inter[ 0 ],
                    "y": 1,
                    "info": { "comments": inter[ 1 ] }
                })


            labels.append( arr_date )
            datasets[ "MILESTONES" ].append({
                "x": arr_date,
                "y": 1
            })

            return {
                "labels": labels,
                "datasets": [{
                    "label": k,
                    "data": datasets[ k ],
                    "pointRadius": 6,
                    "pointHoverRadius": 9,
                } for k in datasets.keys() ]
            }
