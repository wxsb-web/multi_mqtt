import unittest
from unittest.mock import Mock

import rpc_executor
from rpc_executor import PythonExecutor

class rpc_executorTests(unittest.TestCase):
    def test_global(self):
        
        # assert 1
        self.assertEqual(1,1)

e2=None
        
e1 = PythonExecutor(globals=globals())
print(e1)
print(e1.execute('dir(),e1,e2'))
        
e2=PythonExecutor(globals=globals())
print(e2)
print(e2.execute('dir(),e1,e2'))

print('e1===',e1.execute('dir(),e1,e2'))
        
if __name__ == "__main__":
    unittest.main()
        