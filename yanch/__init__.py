
#split on target and control group
def tc(tr, frac=0.1):
    ct=tr.sample(frac=frac)
    tr=tr[~tr.index.isin(ct.index)]
    return tr, ct


#split dataframe by 1M or N rows for excel
def exs(df, max_rows=1000000):
    dataframes = []
    while len(df) > max_rows:
        top = df[:max_rows]
        dataframes.append(top)
        df = df[max_rows:]
    else:
        dataframes.append(df)
    return dataframes



#equalizer of target and control group by basis
def eqg(tr_, ct_, gt=[], cnt=1, replace=False):
    
    tr=tr_.copy()
    ct=ct_.copy()
    reb=False
    ls=[]
    tmp=pd.DataFrame()
    
    
    def stat(tr,ct):
        print('Stats')
        print('Does target spa_key unique? ',tr['spa_key'].is_unique)
        print('Does control spa_key unique? ',ct['spa_key'].is_unique)
        print('Number of rows taget ',tr['spa_key'].count())
        print('Number of rows control ',ct['spa_key'].count())
        if tr['spa_key'].count()==ct['spa_key'].count(): print('Is it equal? True\n') 
        else: print('Is it equal? False\n')
    
    
    tr=tr.assign(counter=tr.groupby(gt).ngroup()+1)
    gt.append('counter')
    ct=pd.merge(ct, tr.drop_duplicates('counter')[gt], on=gt[:len(gt)-1], how='left')
        
    for i in tr['counter'].unique().copy():
        try:
            ls.append(ct[ct.counter==i].sample(n=tr.groupby(by=['counter'])['spa_key'].count()[i]*cnt,replace=replace))
        except:
                reb=True
    tmp=pd.concat(ls, axis=0)
    
    if reb:
        
        print('----------WARNING----------\n')
        print('Do not have enough rows, target will be rebuild to match new control. You can use bootstrap: replace=True\n')
        tr=tr[tr.counter.isin(tmp.counter)]
        stat(tr,tmp)
        return tr.drop('counter', axis=1), tmp.drop('counter', axis=1)
               
    else:
        stat(tr,tmp)
        return tr.drop('counter', axis=1), tmp.drop('counter', axis=1)



# impala select   
def imp_sel(str=''''''):
    cur.execute(str)
    return as_pandas(cur)


# impala insert         
def imp_ins(conn, table, data, into=True, partition = ''):  
    import numpy as np
    def pd_to_impala_types(df):
        impala_types = []
        for name, dtype in zip(df.columns, df.dtypes):
            if "bool" in str(dtype):
                impala_types.append('BOOLEAN')
            elif "float" in str(dtype):
                impala_types.append('DOUBLE')
            elif "int" in str(dtype):
                impala_types.append('BIGINT')
            elif "datetime64" in str(dtype):
                impala_types.append('TIMESTAMP')
            else:
                impala_types.append('STRING')
        return impala_types

    
    def create_table_from_df(conn, table, df, drop=True, partition = ''):
        if drop:
            conn.execute('DROP TABLE IF EXISTS %s'%table)
        
        columns = ', '.join([name + ' ' + impala_type for name, impala_type in zip(df.columns, pd_to_impala_types(df))])
        q = """
            CREATE TABLE %(table)s (
                %(columns)s
            ) %(partition)s STORED AS PARQUET;
        """%{'table':table, 'columns':columns, 'partition':partition}
        conn.execute(q)

    
        
        
    create_table_from_df(conn=conn, table=table, df=data)
        
    q = 'INSERT INTO' if into else 'INSERT OVERWRITE'
    q += """
            %(table)s
            (%(columns)s)
            %(partition)s
        VALUES
            (%(values)s)
    """
    columns = ",".join(data.columns)
    values = ",".join(["("+",".join(['Null' if el is None else (str(el) if isinstance(el, (bool, int, float, np.int64,np.int32 )) else "'" + str(el).replace("'","") + "'")\
                                         for el in r[1].values])+")" for r in data.iterrows()])
    q = q%{'table':table, 'columns':columns, 'values':values, 'partition': partition}
    # print(q)
    conn.execute(q)

#print function 
def p_tc(xn_, metric='ARPU'):
    xn=xn_.copy()
    fig=plt.figure(figsize=(20, 10))
    fig.set_facecolor('#373738')
    text=plt.plot(xn.index, xn.Target, 'r',color='Black')
    plt.plot(xn.index, xn.Control, 'r')

    plt.fill_between(xn.index, xn['Upper band'], xn['Lower band'], color='grey')
    #plt.fill_between(xn.index, xn['Lower band'], xn.Control, color='grey')
    plt.plot([], 'r',color='grey', label='Bands')
    #plt.figtext() 
    ax = plt.gca()
    ax.set_xticks(xn.index) 
    ax.set_facecolor('#373738')

    dda=xn["Date"].map(lambda x: (str(x.day))+'\n'+str(x.month))
    ax.set_xticklabels(dda,ha='center', color='#a6a6a6')
    plt.ylabel(metric).set_color('#a6a6a6')
    plt.xlabel('Date').set_color('#a6a6a6')
    plt.tick_params(axis='y', colors='#a6a6a6')

    plt.legend()
    plt.title(metric).set_color('#a6a6a6')
    plt.grid(False)
    plt.show()
