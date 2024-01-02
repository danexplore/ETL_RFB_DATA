import os
import PySimpleGUI as sg
import pandas as pd


layout = [
    [sg.Text("Pesquisar CNAE:"), sg.InputText(key="-PESQUISAR-", size=(20, 1)), sg.Button("Pesquisar")],
    [sg.Listbox(values=[], size=(40, 10), key="-CNAE_LIST-", select_mode=sg.LISTBOX_SELECT_MODE_SINGLE)],
    [sg.Button("Selecionar"), sg.Button("Fechar")]
]

diretorio_atual = os.getcwd()
arq_cnae = os.path.join(diretorio_atual, 'Dados_RFB', 'Cnae.csv')

cnaes_df = pd.read_csv(arq_cnae, sep=';', header=None)
matriz_cnae = cnaes_df.values.tolist()
print(matriz_cnae)
cnaes = []
for row in matriz_cnae:
    print(row)
    #cnae, descricao = row[0].split(' ', 1)
    #cnaes.append((cnae, descricao))

window = sg.Window("Pesquisa de CNAEs", layout)

while True:
    event, values = window.read()

    if event == sg.WINDOW_CLOSED or event == "Fechar":
        break
    elif event == "Pesquisar":
        valor_pesquisa = values["-PESQUISAR-"].upper()
        cnae_encontrado = [(cnae, desc) for cnae, desc in cnaes if valor_pesquisa in cnae]

        window["-CNAE_LIST-"].update(values=[f"{cnae} - {desc}" for cnae, desc in cnae_encontrado])
    elif event == "Selecionar":
        selected_value = values["-CNAE_LIST-"][0] if values["-CNAE_LIST-"] else None

        if selected_value:
            selected_cnae = selected_value.split(" - ")[0]
            print("CNAE selecionado:", selected_cnae)
            
window.close()
