#!/usr/bin/env python3
"""
Подсчет количества уникальных IPv6-адресов в большом файле.
Программа устойчива к большим объемам данных и ограниченной памяти (1 ГБ).
Использует метод партиционирования по хешу для распределения данных по временным файлам.
"""

import os
import sys
import argparse
import tempfile
import hashlib
import struct
import concurrent.futures
import threading
from pathlib import Path
from typing import List, Tuple, Optional, Set
import mmap
import heapq
from collections import defaultdict

# Константы
IPV6_BYTES_LEN = 16  # IPv6 адрес в бинарном виде занимает 16 байт
TARGET_PARTITION_SIZE = 64 * 1024 * 1024  # 64 МБ на партицию (для безопасной работы в 1 ГБ RAM)
HASH_SEED = 0x71C5E7B3  # Произвольная константа для хеш-функции


class IPv6Parser:
    """Класс для парсинга IPv6 адресов в каноническую бинарную форму."""
    
    @staticmethod
    def to_canonical_bytes(addr_str: str) -> bytes:
        """
        Преобразует IPv6 строку в каноническую бинарную форму (16 байт).
        
        Алгоритм:
        1. Приводим к нижнему регистру
        2. Обрабатываем сжатие '::'
        3. Разбиваем на группы и конвертируем в числа
        4. Упаковываем в 16 байт (big-endian)
        
        Args:
            addr_str: Строка с IPv6 адресом
            
        Returns:
            16 байт в бинарном представлении
        """
        # Приводим к нижнему регистру для единообразия
        addr = addr_str.lower().strip()
        
        # Обработка сжатия '::'
        if '::' in addr:
            return IPv6Parser._expand_compressed(addr)
        
        # Обычный адрес без сжатия
        groups = addr.split(':')
        if len(groups) != 8:
            raise ValueError(f"Неверное количество групп в IPv6 адресе: {addr}")
        
        return IPv6Parser._groups_to_bytes(groups)
    
    @staticmethod
    def _expand_compressed(addr: str) -> bytes:
        """
        Раскрывает сжатый IPv6 адрес с '::'.
        
        Пример: '2001:db0:0:123a::30' -> '2001:0db0:0000:123a:0000:0000:0000:0030'
        """
        parts = addr.split('::')
        if len(parts) != 2:
            raise ValueError(f"Некорректное использование '::' в адресе: {addr}")
        
        left_groups = parts[0].split(':') if parts[0] else []
        right_groups = parts[1].split(':') if parts[1] else []
        
        # Количество пропущенных групп
        missing_groups = 8 - len(left_groups) - len(right_groups)
        if missing_groups < 0:
            raise ValueError(f"Слишком много групп в сжатом адресе: {addr}")
        
        # Собираем полный список групп
        all_groups = left_groups + ['0'] * missing_groups + right_groups
        
        return IPv6Parser._groups_to_bytes(all_groups)
    
    @staticmethod
    def _groups_to_bytes(groups: List[str]) -> bytes:
        """
        Конвертирует список hex-групп в 16 байт.
        """
        result = bytearray()
        for group in groups:
            # Удаляем ведущие нули и конвертируем в число
            # Если группа пустая или '0', используем 0
            if not group or group == '0':
                value = 0
            else:
                # Убираем ведущие нули для парсинга
                value = int(group.lstrip('0') or '0', 16)
            
            # Каждая группа - 2 байта (16 бит)
            result.extend(struct.pack('>H', value))
        
        return bytes(result)


class FastHasher:
    """Быстрая хеш-функция для равномерного распределения по партициям."""
    
    @staticmethod
    def hash_bytes(data: bytes) -> int:
        """
        Вычисляет хеш от бинарных данных.
        Используем модифицированный алгоритм FNV-1a для скорости.
        
        FNV-1a обеспечивает хорошее распределение и очень быстрый.
        """
        hash_val = 0x811c9dc5  # FNV offset basis
        fnv_prime = 0x01000193  # FNV prime
        
        for byte in data:
            hash_val ^= byte
            hash_val *= fnv_prime
            # Ограничиваем до 32 бит для производительности
            hash_val &= 0xffffffff
        
        return hash_val
    
    @staticmethod
    def get_partition(data: bytes, num_partitions: int) -> int:
        """Определяет номер партиции для данных."""
        hash_val = FastHasher.hash_bytes(data)
        return hash_val % num_partitions


class PartitionWriter:
    """Потокобезопасный писатель в партиции."""
    
    def __init__(self, num_partitions: int, temp_dir: str):
        self.num_partitions = num_partitions
        self.temp_dir = temp_dir
        self.files = []
        self.locks = []
        
        # Создаем временные файлы для каждой партиции
        for i in range(num_partitions):
            fd, path = tempfile.mkstemp(dir=temp_dir, suffix=f'.part{i}')
            os.close(fd)  # Закрываем дескриптор, откроем позже через open()
            self.files.append(open(path, 'wb'))
            self.locks.append(threading.Lock())
    
    def write(self, partition: int, data: bytes):
        """Потокобезопасная запись в партицию."""
        with self.locks[partition]:
            self.files[partition].write(data)
    
    def close(self):
        """Закрывает все файлы."""
        for f in self.files:
            f.close()
    
    def get_paths(self) -> List[str]:
        """Возвращает пути к файлам партиций."""
        return [f.name for f in self.files]


class PartitionProcessor:
    """Обработчик одной партиции для подсчета уникальных адресов."""
    
    @staticmethod
    def count_unique(partition_path: str) -> int:
        """
        Подсчитывает количество уникальных IPv6 адресов в партиции.
        
        Алгоритм:
        1. Читаем все записи (каждая по 16 байт)
        2. Сортируем для группировки одинаковых
        3. Линейным проходом подсчитываем уникальные
        
        Если файл слишком большой для памяти, используем внешнюю сортировку.
        """
        file_size = os.path.getsize(partition_path)
        num_records = file_size // IPV6_BYTES_LEN
        
        # Если файл помещается в память (безопасный запас)
        if file_size <= TARGET_PARTITION_SIZE:
            return PartitionProcessor._count_unique_in_memory(partition_path)
        else:
            return PartitionProcessor._count_unique_external(partition_path)
    
    @staticmethod
    def _count_unique_in_memory(partition_path: str) -> int:
        """Подсчет уникальных записей с загрузкой в память."""
        records = []
        with open(partition_path, 'rb') as f:
            while True:
                chunk = f.read(IPV6_BYTES_LEN)
                if not chunk:
                    break
                records.append(chunk)
        
        # Сортируем для группировки одинаковых
        records.sort()
        
        # Подсчет уникальных
        unique_count = 0
        prev = None
        for record in records:
            if record != prev:
                unique_count += 1
                prev = record
        
        return unique_count
    
    @staticmethod
    def _count_unique_external(partition_path: str) -> int:
        """
        Подсчет уникальных записей с использованием внешней сортировки.
        Используется для очень больших партиций.
        """
        # Сортируем файл с помощью внешней сортировки
        sorted_path = PartitionProcessor._external_sort(partition_path)
        
        # Подсчет уникальных линейным проходом
        unique_count = 0
        prev = None
        
        with open(sorted_path, 'rb') as f:
            while True:
                chunk = f.read(IPV6_BYTES_LEN)
                if not chunk:
                    break
                if chunk != prev:
                    unique_count += 1
                    prev = chunk
        
        # Удаляем временный отсортированный файл
        os.unlink(sorted_path)
        
        return unique_count
    
    @staticmethod
    def _external_sort(input_path: str) -> str:
        """
        Внешняя сортировка бинарного файла с записями фиксированной длины.
        Использует алгоритм слияния (merge sort).
        
        Returns:
            Путь к отсортированному файлу
        """
        # Размер блока для сортировки в памяти (64 МБ)
        block_size = 64 * 1024 * 1024
        records_per_block = block_size // IPV6_BYTES_LEN
        
        temp_files = []
        
        # Фаза 1: Разбиение на отсортированные блоки
        with open(input_path, 'rb') as f:
            while True:
                block = []
                for _ in range(records_per_block):
                    record = f.read(IPV6_BYTES_LEN)
                    if not record:
                        break
                    block.append(record)
                
                if not block:
                    break
                
                # Сортируем блок
                block.sort()
                
                # Записываем во временный файл
                fd, temp_path = tempfile.mkstemp()
                with os.fdopen(fd, 'wb') as temp_f:
                    for record in block:
                        temp_f.write(record)
                
                temp_files.append(temp_path)
        
        # Фаза 2: Слияние
        result_fd, result_path = tempfile.mkstemp()
        with os.fdopen(result_fd, 'wb') as result_f:
            # Используем кучу для слияния
            heap = []
            file_handles = []
            
            # Открываем все временные файлы
            for temp_path in temp_files:
                f = open(temp_path, 'rb')
                file_handles.append(f)
                first_record = f.read(IPV6_BYTES_LEN)
                if first_record:
                    heapq.heappush(heap, (first_record, len(file_handles) - 1))
            
            # Сливаем
            while heap:
                record, file_idx = heapq.heappop(heap)
                result_f.write(record)
                
                # Читаем следующий record из того же файла
                next_record = file_handles[file_idx].read(IPV6_BYTES_LEN)
                if next_record:
                    heapq.heappush(heap, (next_record, file_idx))
            
            # Закрываем все файлы
            for f in file_handles:
                f.close()
            
            # Удаляем временные файлы блоков
            for temp_path in temp_files:
                os.unlink(temp_path)
        
        return result_path


class IPv6UniqueCounter:
    """Основной класс для подсчета уникальных IPv6 адресов."""
    
    def __init__(self, memory_limit_mb: int = 1024):
        self.memory_limit_mb = memory_limit_mb
        self.temp_dir = None
    
    def count_unique(self, input_path: str, output_path: str):
        """
        Основной метод подсчета уникальных IPv6 адресов.
        
        Алгоритм:
        1. Определяем количество партиций на основе размера входного файла
        2. Первый проход: читаем файл, парсим адреса, распределяем по партициям
        3. Второй проход: параллельно обрабатываем каждую партицию
        4. Суммируем результаты
        """
        # Создаем временную директорию
        with tempfile.TemporaryDirectory() as temp_dir:
            self.temp_dir = temp_dir
            
            # Определяем количество партиций
            num_partitions = self._calculate_num_partitions(input_path)
            print(f"Используем {num_partitions} партиций", file=sys.stderr)
            
            # Этап 1: Распределение адресов по партициям
            partition_writer = self._distribute_addresses(input_path, num_partitions)
            
            # Этап 2: Параллельная обработка партиций
            total_unique = self._process_partitions(partition_writer.get_paths())
            
            # Этап 3: Запись результата
            with open(output_path, 'w') as f:
                f.write(str(total_unique))
            
            print(f"Всего уникальных адресов: {total_unique}", file=sys.stderr)
    
    def _calculate_num_partitions(self, input_path: str) -> int:
        """
        Вычисляет оптимальное количество партиций на основе размера входного файла.
        """
        file_size = os.path.getsize(input_path)
        
        # Оцениваем количество записей
        avg_line_length = 40  # средняя длина IPv6 строки
        estimated_records = file_size // avg_line_length
        
        # Целевой размер партиции (64 МБ для безопасной работы)
        target_records_per_partition = TARGET_PARTITION_SIZE // IPV6_BYTES_LEN
        
        # Вычисляем необходимое количество партиций
        num_partitions = max(1, estimated_records // target_records_per_partition)
        
        # Ограничиваем сверху, чтобы не создавать слишком много файлов
        # 256 партиций - это разумный максимум для управления
        num_partitions = min(num_partitions, 256)
        
        return num_partitions
    
    def _distribute_addresses(self, input_path: str, num_partitions: int) -> PartitionWriter:
        """
        Первый проход: читает входной файл, парсит адреса и распределяет по партициям.
        Использует memory-mapped файл для эффективного чтения.
        """
        writer = PartitionWriter(num_partitions, self.temp_dir)
        
        # Используем memory-mapped файл для быстрого чтения
        with open(input_path, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Читаем файл построчно
                start = 0
                while True:
                    # Ищем конец строки
                    end = mm.find(b'\n', start)
                    if end == -1:
                        # Последняя строка без перевода строки
                        line = mm[start:].decode('utf-8').strip()
                        if line:
                            self._process_line(line, writer, num_partitions)
                        break
                    
                    # Обрабатываем строку
                    line = mm[start:end].decode('utf-8').strip()
                    if line:  # Пропускаем пустые строки (хотя по условию их нет)
                        self._process_line(line, writer, num_partitions)
                    
                    start = end + 1
        
        writer.close()
        return writer
    
    def _process_line(self, line: str, writer: PartitionWriter, num_partitions: int):
        """
        Обрабатывает одну строку: парсит IPv6 и записывает в соответствующую партицию.
        """
        try:
            # Парсим IPv6 в бинарное представление
            ip_bytes = IPv6Parser.to_canonical_bytes(line)
            
            # Определяем партицию по хешу
            partition = FastHasher.get_partition(ip_bytes, num_partitions)
            
            # Записываем
            writer.write(partition, ip_bytes)
        except Exception as e:
            print(f"Ошибка при обработке строки '{line}': {e}", file=sys.stderr)
            # Продолжаем обработку других строк
    
    def _process_partitions(self, partition_paths: List[str]) -> int:
        """
        Параллельно обрабатывает все партиции для подсчета уникальных адресов.
        """
        total_unique = 0
        
        # Используем ProcessPoolExecutor для параллельной обработки
        # Каждая партиция обрабатывается в отдельном процессе
        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            # Запускаем задачи
            future_to_path = {
                executor.submit(PartitionProcessor.count_unique, path): path
                for path in partition_paths
            }
            
            # Собираем результаты
            for future in concurrent.futures.as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    unique_count = future.result()
                    total_unique += unique_count
                    print(f"Партиция {os.path.basename(path)}: {unique_count} уникальных", 
                          file=sys.stderr)
                except Exception as e:
                    print(f"Ошибка при обработке партиции {path}: {e}", file=sys.stderr)
        
        return total_unique


def main():
    """Точка входа в программу."""
    parser = argparse.ArgumentParser(
        description="Подсчет количества уникальных IPv6 адресов в большом файле"
    )
    parser.add_argument(
        "input_file",
        help="Путь к входному файлу с IPv6 адресами"
    )
    parser.add_argument(
        "output_file",
        help="Путь к выходному файлу для результата"
    )
    parser.add_argument(
        "--memory-limit",
        type=int,
        default=1024,
        help="Лимит памяти в МБ (по умолчанию: 1024)"
    )
    
    args = parser.parse_args()
    
    # Проверяем существование входного файла
    if not os.path.exists(args.input_file):
        print(f"Ошибка: Входной файл '{args.input_file}' не найден", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Создаем счетчик и запускаем обработку
        counter = IPv6UniqueCounter(memory_limit_mb=args.memory_limit)
        counter.count_unique(args.input_file, args.output_file)
        
        print(f"Результат сохранен в '{args.output_file}'", file=sys.stderr)
        
    except Exception as e:
        print(f"Критическая ошибка: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()