import sys
fun_name = lambda n=0: sys._getframe(n + 1).f_code.co_name
from etls.consolidar import consolidar


def test_consolidar():
    event = {
        "report_name" :"dummy_customer",
        "schema"  :"pe",
        "buffer"        :500,
        "drop_workflow" :False 
    }
   
    assert consolidar(event) == True , f"Error  {fun_name()}, {locals()}"
