import pandas as pd
import scipy.stats as sps
from husfort.qutility import qtimer
from husfort.qsqlite import CManagerLibWriter, CTable, CManagerLibReader


def create_numpy_array(nrow: int, ncol: int, cnames: list[str], start_id: int,
                       random_state: int = None) -> pd.DataFrame:
    data = sps.norm.rvs(size=(nrow, ncol), loc=0, scale=1, random_state=random_state)
    df = pd.DataFrame(data=data, columns=cnames)
    df["T"] = [f"T{_:08d}" for _ in range(start_id, nrow + start_id)]
    df = df[["T"] + cnames]
    return df


# --- pandas interface for hdf5
@qtimer
def test_save_to_h5(df: pd.DataFrame, file_path: str, dset_name: str, data_columns: list[str] | bool = None):
    store = pd.HDFStore(file_path, mode="w")
    store.put(key=dset_name, value=df, format="table", data_columns=data_columns)
    store.close()
    return 0


@qtimer
def test_append_to_h5(df: pd.DataFrame, file_path: str, dset_name: str, data_columns: list[str] | bool = None):
    store = pd.HDFStore(file_path, mode="a")
    store.append(key=dset_name, value=df, format="table", data_columns=data_columns)
    store.close()
    return 0


@qtimer
def test_view_h5(file_path: str, dset_name: str) -> pd.DataFrame:
    store = pd.HDFStore(file_path, mode="r")
    df: pd.DataFrame = store.get(key=dset_name)  # type:ignore
    store.close()
    return df


@qtimer
def test_select_from_h5(file_path: str, dset_name: str, conds: list | None = None) -> pd.DataFrame:
    store = pd.HDFStore(file_path, mode="r")
    df: pd.DataFrame = store.select(key=dset_name, where=conds)  # type:ignore
    store.close()
    return df


# --- sql methods to be compared ---
@qtimer
def test_save_to_sql(df: pd.DataFrame, file_path: str, table: CTable, remove_existence: bool):
    lib_writer = CManagerLibWriter(db_save_dir=".", db_name=file_path)
    lib_writer.initialize_table(table=table, remove_existence=remove_existence)
    lib_writer.update(update_df=df, using_index=False)
    lib_writer.commit()
    lib_writer.close()
    return 0


@qtimer
def test_view_sql(file_path: str, dset_name: str, values: list[str]) -> pd.DataFrame:
    lib_reader = CManagerLibReader(db_save_dir=".", db_name=file_path)
    lib_reader.set_default(default_table_name=dset_name)
    df = lib_reader.read(value_columns=values)
    return df


@qtimer
def test_select_from_sql(file_path: str, dset_name: str, values: list[str],
                         conds: list | None = None) -> pd.DataFrame:
    lib_reader = CManagerLibReader(db_save_dir=".", db_name=file_path)
    lib_reader.set_default(default_table_name=dset_name)
    df = lib_reader.read_by_conditions(conditions=conds, value_columns=values)
    return df
