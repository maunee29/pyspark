import pytest

import calculateNum


def test_addmynum():
    assert calculateNum.addmynum(10, 5) == 15

    with pytest.raises(TypeError):
        calculateNum.addmynum("10", 5)


def test_submynum():
    assert calculateNum.submynum(10, 5) == 5

    with pytest.raises(TypeError):
        calculateNum.submynum("10", 5)

def test_mulmynum():
    assert calculateNum.mulmynum(10, 5) == 50

    with pytest.raises(TypeError):
        calculateNum.mulmynum("10", 5)


def test_divmynum():
    c = calculateNum.divmynum(10, 5)
    if c ==2.00:
        print("Code ran fine")
    else:
        print("There was an error")


