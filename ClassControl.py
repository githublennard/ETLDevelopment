class Control:
    '''Clase abstracta control'''

    def apply(self):
        '''Ejecuta el control, los parametros se ponen como atributos de la clase'''
        pass

    def write_in_db(self):
        '''Escribe el control en la base de datos'''
        pass

    