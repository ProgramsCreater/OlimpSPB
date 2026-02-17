#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Программа для деобезличивания данных из Excel-файла.
Алгоритм:
1. Из адреса извлекается ключ (последняя буква перед номером квартиры)
2. Вычисляется смещение относительно буквы 'в'
3. С этим смещением расшифровываются адрес и email
4. С этим же смещением расшифровывается телефон (только строчные английские буквы)
"""

import pandas as pd
import sys
import os

def decrypt_address(address, key_char):
    """
    Расшифровка адреса на основе ключевой буквы
    Возвращает расшифрованный адрес и смещение
    """
    # Вычисляем смещение
    shift = ord(key_char) - ord('в')
    
    decrypted = ""
    for char in address:
        # Заглавные русские буквы
        if 'А' <= char <= 'Я':
            new_pos = ord(char) - shift
            if ord('А') <= new_pos <= ord('Я'):
                decrypted += chr(new_pos)
            elif new_pos < ord('А'):
                decrypted += chr(ord('Я') - (ord('А') - new_pos) + 1)
            else:  # new_pos > ord('Я')
                decrypted += chr(ord('А') + (new_pos - ord('Я')) - 1)
        
        # Строчные русские буквы
        elif 'а' <= char <= 'я':
            new_pos = ord(char) - shift
            if ord('а') <= new_pos <= ord('я'):
                decrypted += chr(new_pos)
            elif new_pos < ord('а'):
                decrypted += chr(ord('я') - (ord('а') - new_pos) + 1)
            else:  # new_pos > ord('я')
                decrypted += chr(ord('а') + (new_pos - ord('я')) - 1)
        
        # Остальные символы оставляем без изменений
        else:
            decrypted += char
    
    return decrypted, -shift  # минус для отображения как в вашем коде

def decrypt_email(email, shift):
    """
    Расшифровка email с заданным смещением
    """
    decrypted = ""
    for char in email:
        # Заглавные английские буквы
        if 'A' <= char <= 'Z':
            new_pos = ord(char) - shift
            if ord('A') <= new_pos <= ord('Z'):
                decrypted += chr(new_pos)
            elif new_pos < ord('A'):
                decrypted += chr(ord('Z') - (ord('A') - new_pos) + 1)
            else:  # new_pos > ord('Z')
                decrypted += chr(ord('A') + (new_pos - ord('Z')) - 1)
        
        # Строчные английские буквы
        elif 'a' <= char <= 'z':
            new_pos = ord(char) - shift
            if ord('a') <= new_pos <= ord('z'):
                decrypted += chr(new_pos)
            elif new_pos < ord('a'):
                decrypted += chr(ord('z') - (ord('a') - new_pos) + 1)
            else:  # new_pos > ord('z')
                decrypted += chr(ord('a') + (new_pos - ord('z')) - 1)
        
        # Остальные символы (цифры, @, ., и т.д.)
        else:
            decrypted += char
    
    return decrypted

def decrypt_phone(phone, shift):
    """
    Расшифровка телефона с заданным смещением.
    Смещаются только строчные английские буквы.
    """
    decrypted = ""
    for char in str(phone):
        # Только строчные английские буквы
        if 'a' <= char <= 'z':
            new_pos = ord(char) - shift
            if ord('a') <= new_pos <= ord('z'):
                decrypted += chr(new_pos)
            elif new_pos < ord('a'):
                decrypted += chr(ord('z') - (ord('a') - new_pos) + 1)
            else:  # new_pos > ord('z')
                decrypted += chr(ord('a') + (new_pos - ord('z')) - 1)
        else:
            decrypted += char
    
    return decrypted

def extract_key_from_address(address):
    """
    Извлекает ключевую букву из адреса
    """
    # Берем последнюю букву перед номером квартиры
    parts = address.split('.')
    if len(parts) >= 3:
        key_part = parts[-2].strip()
        if key_part and key_part[-1].isalpha():
            return key_part[-1]
    
    # Если не нашли через точки, ищем по паттерну с цифрами
    import re
    match = re.search(r'([а-яА-ЯёЁ])[.\s]+(?:\d+)', address)
    if match:
        return match.group(1)
    
    return None

def main():
    """
    Основная функция программы
    """
    # Путь к файлу
    file_path = input("Введите абсолютный путь к файлу (напимер 'C:\\Задание-3-данные.xlsx'):")
    
    # Проверяем существование файла
    if not os.path.exists(file_path):
        print(f"Ошибка: Файл {file_path} не найден")
        print("Укажите правильный путь к файлу или поместите файл в ту же директорию")
        
        # Пробуем найти файл в текущей директории
        local_file = "Задание-3-данные.xlsx"
        if os.path.exists(local_file):
            file_path = local_file
            print(f"Найден локальный файл: {local_file}")
        else:
            return
    
    try:
        # Загружаем данные
        print(f"Загрузка файла: {file_path}")
        data = pd.read_excel(file_path)
        print(f"Загружено строк: {len(data)}")
        
    except Exception as e:
        print(f"Ошибка при загрузке файла: {e}")
        return
    
    # Создаем списки для результатов
    decrypted_addresses = []
    decrypted_emails = []
    decrypted_phones = []
    shifts = []
    key_chars = []
    
    # Пропускаем первую строку (заголовок?) и обрабатываем остальные
    for idx, row in data.iterrows():
        # Получаем данные из колонок (индексы могут отличаться)
        # В вашем файле: Unnamed: 1 - телефон, Unnamed: 2 - email, Unnamed: 3 - адрес
        phone = str(row.iloc[1]) if len(row) > 1 else ""
        email = str(row.iloc[2]) if len(row) > 2 else ""
        address = str(row.iloc[3]) if len(row) > 3 else ""
        
        if not address or pd.isna(address) or address == 'nan':
            print(f"Строка {idx+1}: пустой адрес, пропускаем")
            continue
        
        # Извлекаем ключевую букву из адреса
        key_char = extract_key_from_address(address)
        
        if not key_char:
            print(f"Строка {idx+1}: не удалось извлечь ключ из адреса: {address[:50]}...")
            continue
        
        # Расшифровываем адрес
        decrypted_addr, shift = decrypt_address(address, key_char)
        
        # Расшифровываем email с тем же смещением
        decrypted_eml = decrypt_email(email, -shift)
        
        # Расшифровываем телефон с тем же смещением (только строчные буквы)
        decrypted_ph = decrypt_phone(phone, -shift)
        
        # Сохраняем результаты
        decrypted_addresses.append(decrypted_addr)
        decrypted_emails.append(decrypted_eml)
        decrypted_phones.append(decrypted_ph)
        shifts.append(-shift)  # минус для отображения как в вашем коде
        key_chars.append(f"{key_char} -> {-shift}")
        
        # Выводим прогресс
        print(f"\nСтрока {idx+1}:")
        print(f"  Ключ: '{key_char}' -> смещение { -shift}")
        print(f"  Адрес: {decrypted_addr}")
        print(f"  Email: {decrypted_eml}")
        print(f"  Телефон: {decrypted_ph}")
    
    # Создаем датасет с результатами
    result_df = pd.DataFrame({
        'Телефон (расшифрованный)': decrypted_phones,
        'Email (расшифрованный)': decrypted_emails,
        'Адрес (расшифрованный)': decrypted_addresses,
        'Ключ шифрования (буква → смещение)': key_chars,
        'Смещение': shifts
    })
    
    # Сохраняем в новый Excel-файл
    output_file = "деобезличенные_данные.xlsx"
    result_df.to_excel(output_file, index=False)
    
    print(f"\n" + "="*60)
    print(f"Обработано строк: {len(decrypted_addresses)}")
    print(f"Результаты сохранены в файл: {output_file}")
    print("="*60)
    
    # Показываем первые несколько строк результата
    print("\nПервые 5 расшифрованных записей:")
    print(result_df.head(5).to_string())

def test_with_sample():
    """
    Тестовая функция с примером из вашего кода
    """
    # Тестовые данные
    test_address = "въ. Псчпщэьабявщбэяп Ычщэоьп у.5 щс.476"
    test_email = "xgdlt@bpgfjpgsi.qxo"
    test_phone = "47f0896ebfa7f70683e82c26bfdba33178d1d6a8"
    
    key_char = extract_key_from_address(test_address)
    print(f"Тест: ключевая буква = '{key_char}'")
    
    decrypted_addr, shift = decrypt_address(test_address, key_char)
    decrypted_email = decrypt_email(test_email, shift)
    decrypted_phone = decrypt_phone(test_phone, shift)
    
    print(f"Смещение: { -shift}")
    print(f"Адрес: {decrypted_addr}")
    print(f"Email: {decrypted_email}")
    print(f"Телефон: {decrypted_phone}")

if __name__ == "__main__":
    print("ПРОГРАММА ДЕОБЕЗЛИЧИВАНИЯ ДАННЫХ")
    print("="*60)
    
    # Раскомментируйте для теста на одном примере
    # test_with_sample()
    
    # Запуск основной программы
    main()