"""
conclusion:
0.  parameter data_columns is a key factor to effect the performance. This parameter means List of columns to create
    as indexed data columns for on-disk queries, or True to use all columns. By default, only the axes of the object
    are indexed.
1.  nrow > 1_000_000, h5 > sql
2.  nrow <   100_000, h5 < sql
"""
if __name__ == "__main__":
    from loguru import logger
    from husfort.qsqlite import CTable
    from husfort.qlog import define_logger
    from test_funs_and_modules import create_numpy_array
    from test_funs_and_modules import test_save_to_h5, test_save_to_sql
    from test_funs_and_modules import test_view_h5, test_view_sql
    from test_funs_and_modules import test_select_from_h5, test_select_from_sql
    from test_funs_and_modules import test_append_to_h5

    define_logger()

    nrow, ncol = 100_000, 10
    cnames = [f"C{_:02d}" for _ in range(ncol)]
    df1 = create_numpy_array(nrow, ncol, cnames=cnames, start_id=0)
    df2 = create_numpy_array(nrow, ncol, cnames=cnames, start_id=nrow)
    print(df1)
    print(df2)
    h5_file, sql_file = "test.h5", "test.db"
    dset_name = "my_test_dataset"
    threshold_date = f"T{int(0.5 * nrow * 2):08d}"
    data_columns = ["T", "C00", "C01"]

    logger.info("--- test 1: save ---")
    test_save_to_h5(df=df1, file_path=h5_file, dset_name=dset_name, data_columns=data_columns)
    table = CTable(
        table_struct={
            "table_name": dset_name,
            "primary_keys": {"T": "TEXT"},
            "value_columns": {_: "REAL" for _ in cnames},
        }
    )
    test_save_to_sql(df=df1, file_path=sql_file, table=table, remove_existence=True)

    logger.info("--- test 2: append ---")
    test_append_to_h5(df=df2, file_path=h5_file, dset_name=dset_name, data_columns=data_columns)
    test_save_to_sql(df=df2, file_path=sql_file, table=table, remove_existence=False)

    logger.info("--- test 3: view ---")
    df3 = test_view_h5(file_path=h5_file, dset_name=dset_name)
    df4 = test_view_sql(file_path=sql_file, dset_name=dset_name, values=["T"] + cnames)
    logger.info(f"df_h5  shape={df3.shape}")
    logger.info(f"df_sql shape={df4.shape}")
    print(df3.head(5))
    print(df4.head(5))

    logger.info("--- test 4: select ---")
    slc_data1 = test_select_from_h5(
        file_path=h5_file, dset_name=dset_name,
        conds=[
            # "C00 > 0 | C02 > 0",
            "C00 > 0",
            "C01 < 0",
            f"T > {threshold_date}"
        ])
    slc_data2 = test_select_from_sql(
        file_path=sql_file, dset_name=dset_name, values=["T"] + cnames,
        conds=[
            ("C00", ">", 0),
            ("C01", "<", 0),
            ("T", ">", threshold_date),
        ])
    logger.info(f"selected h5  shape={slc_data1.shape}")
    logger.info(f"selected sql shape={slc_data2.shape}")
    print(slc_data1)
    print(slc_data2)
