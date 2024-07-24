import h5py
import numpy as np


def test_the_initialization() -> None:
    with h5py.File("mytestfile.hdf5", "w") as f:
        dset = f.create_dataset("mydataset", (100,), dtype='i')
        sub_grp = f.create_group("subgroup")
        sub_sub_grp = sub_grp.create_group("subsubgroup")
        another_dset1 = sub_grp.create_dataset("another_dataset", (100, 8), dtype="f")
        another_dset2 = sub_sub_grp.create_dataset("another_dataset", (100, 8), dtype="f")
        print(f.name)
        print(dset.name)
        print(dset.shape)
        print(dset.dtype)
        print(sub_grp.name)
        print(sub_sub_grp.name)
        print(another_dset1.name)
        print(another_dset2.name)

        print("-" * 12)
        for name in f:
            print(name)

        print("-" * 12)
        print(f"subgroup in f is {'subgroup' in f}")
        print(f"subsubgroup in f is {'subsubgroup' in f}")
