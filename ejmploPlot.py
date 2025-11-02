import matplotlib.pyplot as plt
import numpy as np

# 1. Crear datos para el gráfico
# Generamos 100 puntos igualmente espaciados entre 0 y 2*pi
x = np.linspace(0, 2 * np.pi, 100)
# Calculamos el valor del seno para cada punto x
y = np.sin(x)

# 2. Crear el gráfico
plt.figure(figsize=(8, 6)) # Opcional: define el tamaño de la figura

# Usar la función plot para dibujar la línea
plt.plot(x, y, label='sen(x)', color='blue', linestyle='-', linewidth=2)

# 3. Añadir etiquetas y título (buenas prácticas para cualquier gráfico)
plt.title('Gráfico de la función Seno')
plt.xlabel('Eje X (Ángulo en radianes)')
plt.ylabel('Eje Y (Valor de sen(x))')
plt.grid(True) # Añadir una cuadrícula para facilitar la lectura
plt.legend() # Mostrar la leyenda definida en plt.plot()

# 4. Mostrar el gráfico
plt.show()