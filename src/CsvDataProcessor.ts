import csv from 'csv-parser'
import fs from 'fs'
import { ICSVData, IData, IError } from './types'

export class CsvDataProcessor {
  filePath: string = '';
  batchSize: number = 1000
  success_results: IData[] = []
  error_results: IError[] = []
  count: number = 0

  constructor(filePath: string, batchSize?: number) {
    this.filePath = filePath

    if (batchSize) {
      this.batchSize = batchSize
    }
  }

  resetData() {
    this.success_results = []
    this.error_results = []
    this.count = 0
  }

  proccessBatch() {
    // Esta função vai receber os dados em lote, de acordo com o tamanho do lote informado no construtor.
    // Nela, podemos inserir os dados validados no banco de dados, e também salvar o log dos registros com erro.
    // Utilizei a estratégia de lotes async para não armazenar muita informação em memória,
    // permitindo assim o processamento em larga escala.
    console.log('Registros com erro', this.error_results)
    console.log('Registros corretos', this.success_results)
    this.resetData()
  }

  validateCPF(cpf: string): boolean {
    // Inicia a validacao deixando somente os números
    cpf = cpf.replace(/\D/g, '');

    // Valida se o CPF tem 11 digitos e se os digitos nao sao repetidos
    if (cpf.length !== 11 || /^(\d)\1{10}$/.test(cpf)) {
      return false;
    }

    // Função que calcula o dígito verificador
    const calcularDigito = (numeros: string, pesos: number[]): number => {
      let soma = 0;
      for (let i = 0; i < numeros.length; i++) {
        soma += parseInt(numeros[i]) * pesos[i];
      }
      const resto = soma % 11;
      return resto < 2 ? 0 : 11 - resto;
    };

    // Valida o primeiro dígito verificador
    const pesosPrimeiroDigito = [10, 9, 8, 7, 6, 5, 4, 3, 2];
    const primeiroDigito = calcularDigito(cpf.substring(0, 9), pesosPrimeiroDigito);

    if (parseInt(cpf[9]) !== primeiroDigito) {
      return false;
    }

    // Valida o segundo dígito verificador
    const pesosSegundoDigito = [11, 10, 9, 8, 7, 6, 5, 4, 3, 2];
    const segundoDigito = calcularDigito(cpf.substring(0, 10), pesosSegundoDigito);

    return parseInt(cpf[10]) === segundoDigito;
  }

  validateCNPJ(cnpj: string): boolean {
    // Inicia a validacao deixando somente os números
    cnpj = cnpj.replace(/\D/g, '');

    // Valida se o CNPJ tem 14 digitos e se os digitos nao sao repetidos
    if (cnpj.length !== 14 || /^(\d)\1{13}$/.test(cnpj)) {
      return false;
    }

    // Função que calcula o dígito verificador
    const calcularDigitoCNPJ = (numeros: string, pesos: number[]): number => {
      let soma = 0;
      for (let i = 0; i < numeros.length; i++) {
        soma += parseInt(numeros[i]) * pesos[i];
      }
      const resto = soma % 11;
      return resto < 2 ? 0 : 11 - resto;
    };

    // Calcula o primeiro dígito verificador
    const pesosPrimeiroDigito = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
    const primeiroDigito = calcularDigitoCNPJ(cnpj.substring(0, 12), pesosPrimeiroDigito);

    if (parseInt(cnpj[12]) !== primeiroDigito) {
      return false;
    }

    // Valida o segundo dígito verificador
    const pesosSegundoDigito = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
    const segundoDigito = calcularDigitoCNPJ(cnpj.substring(0, 13), pesosSegundoDigito);

    return parseInt(cnpj[13]) === segundoDigito;
  }

  validateInstallments(data: ICSVData): boolean {
    return (parseFloat(data.vlTotal) / parseInt(data.qtPrestacoes)) === parseFloat(data.vlPresta)
  }

  formatCurrency(value: string) {
    return new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(parseFloat(value));
  }

  proccess(data: ICSVData) {
    this.count++

    if (![11, 14].includes(data.nrCpfCnpj.replace(/\D/g, '').length)) {
      return this.error_results.push({
        error: 'Invalid CPF or CNPJ',
        data
      })
    }

    if (data.nrCpfCnpj.length === 11 && !this.validateCPF(data.nrCpfCnpj)) {
      return this.error_results.push({
        error: 'Invalid CPF',
        data
      })
    } else if (data.nrCpfCnpj.length === 14 && !this.validateCNPJ(data.nrCpfCnpj)) {
      return this.error_results.push({
        error: 'Invalid CNPJ',
        data
      })
    }

    if (!this.validateInstallments(data)) {
      return this.error_results.push({
        error: 'Invalid Installments',
        data
      })
    }

    this.success_results.push({
      ...data,
      nrInst: parseInt(data.nrInst, 10),
      nrAgencia: parseInt(data.nrAgencia, 10),
      cdClient: parseInt(data.cdClient, 10),
      nrContrato: parseInt(data.nrContrato, 10),
      qtPrestacoes: parseInt(data.qtPrestacoes, 10),
      vlTotal: this.formatCurrency(data.vlTotal),
      cdProduto: parseInt(data.cdProduto, 10),
      cdCarteira: parseInt(data.cdCarteira, 10),
      nrProposta: parseInt(data.nrProposta, 10),
      nrPresta: parseInt(data.nrPresta, 10),
      nrSeqPre: parseInt(data.nrSeqPre, 10),
      vlPresta: this.formatCurrency(data.vlPresta),
      vlMora: this.formatCurrency(data.vlMora),
      vlMulta: this.formatCurrency(data.vlMulta),
      vlOutAcr: this.formatCurrency(data.vlOutAcr),
      vlIof: this.formatCurrency(data.vlIof),
      vlDescon: this.formatCurrency(data.vlDescon),
      vlAtual: this.formatCurrency(data.vlAtual),
    })
  }

  async getCsvData(): Promise<void> {
    fs.createReadStream(this.filePath)
      .pipe(csv())
      .on('data', (data: ICSVData) => {
        this.proccess(data)

        if (this.count >= this.batchSize) {
          this.proccessBatch()
        }
      })
      .on('end', () => {
        this.proccessBatch()
      })
  }

  async run() {
    this.resetData()
    await this.getCsvData()
  }
}
