# C:\Users\juanf\Downloads\sweetsol\ss_backend\venv\Scripts

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from enum import Enum

import psycopg as pg
from datetime import date, datetime, timedelta
import calendar

from pydantic import BaseModel


CONN_ARGS = {
    "host": "localhost",
    "port": "5432",
    "dbname": "mmontoya",
    "user": "postgres",
    "password": "postgres"
}


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins = [ "*" ],
    allow_methods = [ "*" ],
    allow_headers = [ "*" ]
)


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
    jan = calendar.monthcalendar( year, 1 )
    if( jan[ 0 ][ 0 ] ):
        low = date(
            year,
            1,
            jan[ 0 ][ 0 ]
        )
    else:
        low = date(
            year - 1,
            12,
            jan[ 1 ][ 0 ]
        )
    
    dec = calendar.monthcalendar( year, 12 )
    if( dec[ -1 ][ -1 ] ):
        high = date(
            year,
            12,
            dec[ -1 ][ -1 ]
        )
    else:
        high = date(
            year + 1,
            1,
            calendar.monthcalendar( year + 1, 1 )[ 0 ][ -1 ]
        )
    
    return ( low, high )

def set_limits_for_month( year, month ):
    if( month == 1 ):
        next = ( year, 2 )
    elif( month == 12 ):
        next = ( year + 1, 1 )
    else:
        next = ( year, month + 1 )

    curr_month = calendar.monthcalendar( year, month )

    if( curr_month[ 0 ][ 0 ] ):
        low = date(
            year,
            month,
            curr_month[ 0 ][ 0 ]
        )
    else:
        low = date(
            year,
            month,
            curr_month[ 1 ][ 0 ]
        )
    
    if( curr_month[ -1 ][ -1 ] ):
        high = date(
            year,
            month,
            curr_month[ -1 ][ -1 ]
        )
    else:
        high = date(
            next[ 0 ],
            next[ 1 ],
            calendar.monthcalendar( next[ 0 ], next[ 1 ] )[ 0 ][ -1 ]
        )
    
    return ( low, high )

def set_limits_for_week( year, month, week ):
    week_days = calendar.monthcalendar( year, month )[ week ]

    if( week_days[ 0 ] ):
        low = date( year, month, week_days[ 0 ] )
        high = low + timedelta( days = 6 )
    else:
        if( week == 0 ):
            if( month == 1 ):
                year -= 1
                month = 12
                week = -1
            else:
                month -= 1
                week = -1
        else:
            week -=1
        
        week_days = calendar.monthcalendar( year, month )[ week ]

        low = date( year, month, week_days[ 0 ] )
        high = low + timedelta( days = 6 )

    return ( low, high )

# def build_dataset_for_year( data, year ):
#     base = { "CONTACT": 0, "DEPOSIT": 0, "ARRIVAL": 0 }
#     ds = { datetime( year, i, 1 ).strftime( "%Y-%m-%d" ): base for i in range( 1, 13 ) }

#     for row in data:
#         ds[ row[ 0 ].strftime( "%Y-%m-%d" ) ][ row[ 1 ] ] = row[ 2 ]

#     return ds

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

def build_dataset_for_year( data, year, filters ):
    base = { "CONTACT": 0, "DEPOSIT": 0, "ARRIVAL": 0 }
    ds = { datetime( year, i, 1 ).strftime( "%Y-%m-%d" ): base.copy() for i in range( 1, 13 ) }

    for row in data:
        ds[ row[ 0 ].strftime( "%Y-%m-%d" ) ][ row[ 1 ] ] = row[ 2 ]
    
    labels = []
    dsets_data = { f: [] for f in filters }

    for k in ds.keys():
        labels.append( k )
        for f in filters:
            dsets_data[ f ].append( ds[ k ][ f ] )

    return {
        "labels": MONTHS,
        "datasets": [
            { "label": k, "data": dsets_data[ k ] } for k in dsets_data.keys()
        ]
    }

def build_dataset_for_month( data, year, month, filters ):
    ds = {}

    if( month == 1 ):
        next = ( year, 2 )
    elif( month == 12 ):
        next = ( year + 1, 1 )
    else:
        next = ( year, month + 1 )
    
    curr_month = calendar.monthcalendar( year, month )

    for i in range( 0, len( curr_month ) - 1 ):
        if( curr_month[ i ][ 0 ] ):
            ds[ date( year, month, curr_month[ i ][ 0 ] ).strftime( "%Y-%m-%d" ) ] = {
                "CONTACT": 0,
                "DEPOSIT": 0,
                "ARRIVAL": 0
            }
        else:
            ds[ date( year, month, curr_month[ i + 1 ][ 0 ] ).strftime( "%Y-%m-%d" ) ] = {
                "CONTACT": 0,
                "DEPOSIT": 0,
                "ARRIVAL": 0
            }

    if( curr_month[ -1 ][ 0 ] ):
        ds[ date( year, month, curr_month[ -1 ][ 0 ] ).strftime( "%Y-%m-%d" ) ] = {
            "CONTACT": 0,
            "DEPOSIT": 0,
            "ARRIVAL": 0
        }
    else:
        next_month = calendar.monthcalendar( next[ 0 ], next[ 1 ] )
        ds[ date( next[ 0 ], next[ 1 ], next_month[ 0 ][ -1 ] ).strftime( "%Y-%m-%d" ) ] = {
            "CONTACT": 0,
            "DEPOSIT": 0,
            "ARRIVAL": 0
        }

    for row in data:
        ds[ row[ 0 ].strftime( "%Y-%m-%d" ) ][ row[ 1 ] ] = row[ 2 ]

    labels = []
    dsets_data = { f: [] for f in filters }

    for k in ds.keys():
        labels.append( k )
        for f in filters:
            dsets_data[ f ].append( ds[ k ][ f ] )

    return {
        "labels": [ f"S{ i }" for i in range( 1, len( labels ) + 1 ) ],
        "datasets": [
            { "label": k, "data": dsets_data[ k ] } for k in dsets_data.keys()
        ]
    }

WEEK_DAYS = [
    "Lun",
    "Mar",
    "Mie",
    "Jue",
    "Vie",
    "Sab",
    "Dom"
]

def build_dataset_for_week( data, year, month, week, filters ):
    ds = {}

    l, _ = set_limits_for_week( year, month, week )
    for i in range( 0, 7 ):
        ds[ ( l + timedelta( days = i * 1 ) ).strftime( "%Y-%m-%d" ) ] = {
            "CONTACT": 0,
            "DEPOSIT": 0,
            "ARRIVAL": 0
        }

    for row in data:
        ds[ row[ 0 ].strftime( "%Y-%m-%d" ) ][ row[ 1 ] ] = row[ 2 ]

    labels = []
    dsets_data = { f: [] for f in filters }

    for k in ds.keys():
        labels.append( k )
        for f in filters:
            dsets_data[ f ].append( ds[ k ][ f ] )

    return {
        "labels": WEEK_DAYS,
        "datasets": [
            { "label": k, "data": dsets_data[ k ] } for k in dsets_data.keys()
        ]
    }

@app.get( "/interactions/count" )
def interactions_count(
    year: int = None,
    month: int = None,
    week: int = None,

    exc_cont: bool = False,
    exc_dep: bool = False,
    exc_arr: bool = False,
):
    if( not year ):
        raise HTTPException( status_code = 422 )
    if( not month and week != None ):
        raise HTTPException( status_code = 422 )
    
    next_year = year
    if( month and week != None ):
        l, h = set_limits_for_week( year, month, week )
        trunc = "day"
        span = f"""
            and i.inter_date >= '{ l }'
            and i.inter_date <= '{ h }'
        """
    elif( month ):
        l, h = set_limits_for_month( year, month )
        trunc = "week"
        span = f"""
            and i.inter_date >= '{ l }'
            and i.inter_date <= '{ h }'
        """
    else:
        trunc = "month"
        l, h = set_limits_for_year( year )
        span = f"""
            and i.inter_date >= '{ l }'
            and i.inter_date <= '{ h }'
        """

    filters = []
    if( not exc_cont ): filters.append( "CONTACT" )
    if( not exc_dep ): filters.append( "DEPOSIT" )
    if( not exc_arr ): filters.append( "ARRIVAL" )

    if( not len( filters ) ):
        return { "labels": [], "datasets": [] }

    filters_str = "(" + "".join( list( map( lambda e: f"'{ e }', ", filters ) ) )[ : -2 ] + ")"

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            # print(f"""
            #     select date_trunc('{ trunc }', i.inter_date) as { trunc }, i.milestone_type, count(*)
            #     from main.interaction i
            #     where i.milestone_type in { filters }
            #     { span }
            #     group by { trunc }
            # """)

            cur.execute(f"""
                select date_trunc('{ trunc }', i.inter_date) as { trunc }, i.milestone_type, count(*)
                from main.interaction i
                where i.milestone_type in { filters_str }
                { span }
                group by { trunc }, i.milestone_type
                order by { trunc }
            """)

            if( month != None and week != None ):
                return build_dataset_for_week( cur.fetchall(), year, month, week, filters )
            elif( month != None ):
                return build_dataset_for_month( cur.fetchall(), year, month, filters )
            else:
                return build_dataset_for_year( cur.fetchall(), year, filters )


@app.get( "/interactions" )
def interactions(
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = None,

    exc_cont: bool = False,
    exc_dep: bool = False,
    exc_arr: bool = False,
):
    if( not year ):
        raise HTTPException( status_code = 422 )
    if( not month and week != None ):
        raise HTTPException( status_code = 422 )
    if( ( not month or week == None ) and day != None ):
        raise HTTPException( status_code = 422 )
    
    if( month and week != None and day != None ):
        mc = calendar.monthcalendar( year, month )
        if( mc[ week ][ 0 ] ):
            md = mc[ week ][ day ]
        else:
            md = mc[ 1 ][ day ]

        l = date( year, month, md )
        h = l + timedelta( hours = 23, minutes = 59, seconds = 59 )

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
    if( not exc_cont ): filters.append( "CONTACT" )
    if( not exc_dep ): filters.append( "DEPOSIT" )
    if( not exc_arr ): filters.append( "ARRIVAL" )

    if( not len( filters ) ):
        return []

    filters_str = "(" + "".join( list( map( lambda e: f"'{ e }', ", filters ) ) )[ : -2 ] + ")"

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            # print(f"""
            #     select date_trunc('{ trunc }', i.inter_date) as { trunc }, i.milestone_type, count(*)
            #     from main.interaction i
            #     where i.milestone_type in { filters }
            #     { span }
            #     group by { trunc }
            # """)

            cur.execute(f"""
                select c.vid, c.name, c.phone_num, c.email, i.milestone_type, i.inter_date, i.inter_desc, i.checked
                from main.client c, main.interaction i
                where i.client_vid = c.vid
                and i.milestone_type in { filters_str }
                { span }
            """)

            ret = []
            for row in cur.fetchall():
                names = row[ 1 ].split()
                ret.append({
                    "client": {
                        "vid": row[ 0 ],
                        "name": names[ 0 ],
                        "lastname": names[ 1 ],
                        "phone": row[ 2 ],
                        "email": row[ 3 ]
                    },
                    
                    "interaction": {
                        "milestone_type": row[ 4 ],
                        "inter_date": row[ 5 ].strftime( "%Y-%m-%d" ),
                        "inter_desc": row[ 6 ],
                        "checked": row[ 7 ]
                    }
                })

            return ret


@app.post( "/interactions/checked/toogle" )
def interactions_checked_toogle(
    client_vid: int,
    milestone_type: str,
    inter_date: str
):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                update main.interaction
                set checked = not checked
                where interaction.client_vid = { client_vid }
                and interaction.milestone_type = '{ milestone_type }'
                and interaction.inter_date = '{ inter_date }'
                returning checked
            """)

            return { "new_value": cur.fetchone()[ 0 ] }


# @app.get( "/client/{client_vid}/roadmap" )
# def client_roadmap( client_vid: int ):
#     with pg.connect( **CONN_ARGS ) as conn:
#         with conn.cursor() as cur:
#             cur.execute(f"""
#                 select c.vid, c.name, c.phone_num, c.email, i.milestone_type, i.inter_date, i.inter_desc, i.checked
#                 from main.client c, main.interaction i
#                 where i.client_vid = c.vid
#                 and i.client_vid = { client_vid }
#             """)

#             ret = []
#             for row in cur.fetchall():
#                 names = row[ 1 ].split()
#                 ret.append({
#                     "client": {
#                         "vid": row[ 0 ],
#                         "name": names[ 0 ],
#                         "lastname": names[ 1 ],
#                         "phone": row[ 2 ],
#                         "email": row[ 3 ]
#                     },
                    
#                     "interaction": {
#                         "milestone_type": row[ 4 ],
#                         "inter_date": row[ 5 ].strftime( "%Y-%m-%d" ),
#                         "inter_desc": row[ 6 ],
#                         "checked": row[ 7 ]
#                     }
#                 })

#             return ret


@app.get( "/client/{client_vid}/roadmap" )
def client_roadmap( client_vid: int ):
    def milestone_floors( ms_type ):
        ret = 1

        if( ms_type == "DEPOSIT" ):
            ret = 2
        elif( ms_type == "ARRIVAL" ):
            ret = 3
        
        return ret


    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            datasets = { "CONTACT": [], "DEPOSIT": [], "ARRIVAL": [] }

            cur.execute(f"""
                select c.vid, c.name, c.phone_num, c.email, i.milestone_type, to_char(i.inter_date, 'YYYY-MM-DD'), i.inter_desc, i.checked
                from main.client c, main.interaction i
                where i.client_vid = c.vid
                and i.client_vid = { client_vid }
                order by i.inter_date
            """)

            interactions = cur.fetchall()

            cur.execute(f"""
                select cm.client_vid, cm.milestone_type, to_char(cm.date, 'YYYY-MM-DD')
                from main.client_milestone cm
                where cm.client_vid = { client_vid }
                order by cm.date
            """)

            milestones = list( cur.fetchall() )

            datasets[ "MILESTONE" ] = []

            datasets = {
                "CONTACT": [],
                "DEPOSIT": [],
                "ARRIVAL": [],
                "MILESTONE": [],
            }

            labels = []

            i = 0
            while( len( milestones ) and i < len( interactions ) ):
                while(
                    len( milestones ) and
                    i < len( interactions ) and
                    interactions[ i ][ 5 ] == milestones[ 0 ][ 2 ]
                ):
                    datasets[ interactions[ i ][ 4 ] ].append({
                        "x": interactions[ i ][ 5 ],
                        "y": milestone_floors( interactions[ i ][ 4 ] ),
                        "info": { "comments": interactions[ i ][ 6 ] }
                    })
                    i += 1
                
                labels.append( milestones[ 0 ][ 2 ] )
                datasets[ "MILESTONE" ].append( { "x": milestones[ 0 ][ 2 ], "y": 0 } )
                milestones.pop( 0 )

                while(
                    len( milestones ) and
                    i < len( interactions ) and
                    interactions[ i ][ 5 ] < milestones[ 0 ][ 2 ]
                ):
                    labels.append( interactions[ i ][ 5 ] )
                    datasets[ interactions[ i ][ 4 ] ].append({
                        "x": interactions[ i ][ 5 ],
                        "y": milestone_floors( interactions[ i ][ 4 ] ),
                        "info": { "comments": interactions[ i ][ 6 ] }
                    })
                    i += 1

            if( i < len( interactions ) ):
                for row in interactions[ i : ]:
                    labels.append( row[ 5 ] )
                    datasets[ row[ 4 ] ].append({
                        "x": row[ 5 ],
                        "y": milestone_floors( row[ 4 ] ),
                        "info": { "comments": row[ 6 ] }
                    })
            else:
                for row in milestones:
                    labels.append( row[ 2 ] )
                    datasets[ "MILESTONE" ].append( { "x": row[ 2 ], "y": 0 } )

            return {
                "labels": labels,
                "datasets": [{
                    "label": k,
                    "data": datasets[ k ],
                    "pointRadius": 6,
                    "pointHoverRadius": 9,
                } for k in datasets.keys() ]
            }


class MilestoneType( Enum ):
    CONTACT = "CONTACT"
    DEPOSIT = "DEPOSIT"
    ARRIVAL = "ARRIVAL"

class Interaction( BaseModel ):
    client_vid: int
    milestone_type: MilestoneType
    inter_date: str
    inter_desc: str

@app.post( "/interactions/create" )
def create_interaction( interaction: Interaction ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                insert into main.interaction values(
                    { interaction.client_vid },
                    '{ interaction.milestone_type.value }',
                    '{ interaction.inter_date }',
                    '{ interaction.inter_desc }',
                    false
                )
            """)


@app.get( "/agents" )
def agents():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select * from main.agent
            """)

            return cur.fetchall()