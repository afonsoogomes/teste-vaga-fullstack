import { CsvDataProcessor } from './CsvDataProcessor';

// Inicio o script, onde passo o caminho do arquivo CSV e o tamanho do lote como parâmetro
const csvDataProcessor = new CsvDataProcessor('./data.csv', 1000)
csvDataProcessor.run()
