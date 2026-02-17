import cv2
import numpy as np

def apply_color_transfer(source, target, mask):
    """
    Переносит цветовую статистику с target на source в пространстве Lab
    только для области, заданной маской.
    """
    if np.sum(mask) == 0:
        return source  # Нет пикселей для обработки

    # Конвертируем в float для точности вычислений
    source_float = source.astype(np.float32) / 255.0
    target_float = target.astype(np.float32) / 255.0

    # Конвертируем в Lab
    source_lab = cv2.cvtColor(source_float, cv2.COLOR_BGR2Lab)
    target_lab = cv2.cvtColor(target_float, cv2.COLOR_BGR2Lab)
    result_lab = source_lab.copy()

    # Применяем перенос цвета только по маске
    for i in range(3):  # Для каналов L, a, b
        source_channel = source_lab[:, :, i][mask > 0]
        target_channel = target_lab[:, :, i].flatten() # Берем всю статистику с таргета (или тоже можно по маске, но сложнее)

        # Можно взять статистику по всему таргету, либо по его маске, если мы ее построили.
        # Для простоты возьмем по всему таргету.
        mean_target = np.mean(target_channel)
        std_target = np.std(target_channel)

        mean_source = np.mean(source_channel)
        std_source = np.std(source_channel)

        if std_source == 0:
            std_source = 1 # Защита от деления на ноль

        # Создаем временный слой
        layer = source_lab[:, :, i].copy()
        # Применяем формулу только к маскированным пикселям
        layer[mask > 0] = ((layer[mask > 0] - mean_source) *
                          (std_target / std_source) + mean_target)
        result_lab[:, :, i] = layer

    # Конвертируем обратно в BGR
    result_bgr = cv2.cvtColor(result_lab, cv2.COLOR_Lab2BGR)
    return np.clip(result_bgr * 255, 0, 255).astype(np.uint8)


def create_foliage_mask(image, season='autumn'):
    """
    Создает маску для листвы на основе цвета в HSV.
    season: 'autumn' (желтый/красный) или 'summer' (зеленый).
    """
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)

    if season == 'autumn':
        # Диапазон для желто-красных тонов (Hue 10-85)
        lower = np.array([0, 20, 20])
        upper = np.array([85, 255, 255])
    else: # summer
        # Диапазон для зеленого (Hue 35-85)
        lower = np.array([35, 20, 20])
        upper = np.array([90, 255, 255])

    mask = cv2.inRange(hsv, lower, upper)

    # Морфологическая очистка
    kernel = np.ones((3, 3), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)

    return mask


def transform_season(input_path, donor_path, output_path, input_season, donor_season):
    """
    Основная функция трансформации.
    input_season: сезон исходного фото ('summer' или 'autumn')
    donor_season: сезон фото-донора (противоположный)
    """
    # 1. Загрузка
    img_src = cv2.imread(input_path)
    img_donor = cv2.imread(donor_path)

    if img_src is None or img_donor is None:
        print("Ошибка загрузки изображений")
        return

    # 2. Сегментация листвы на исходном изображении
    foliage_mask = create_foliage_mask(img_src, input_season)

    # 3. Расширяем маску до 3 каналов (для удобства умножения)
    mask_3channel = cv2.merge([foliage_mask, foliage_mask, foliage_mask]) / 255

    # 4. Изолируем фон и листву (фон нам не нужен для расчета статистики, но пригодится для сборки)
    #    Умножаем исходник на инвертированную маску, чтобы получить фон.
    background = img_src * (1 - mask_3channel)

    # 5. Применяем перенос цвета к исходному изображению,
    #    но ограничиваясь маской внутри функции apply_color_transfer.
    #    ВНИМАНИЕ: мы передаем всё изображение, но функция меняет только пиксели под маской.
    img_transformed = apply_color_transfer(img_src, img_donor, foliage_mask)

    # 6. Выделяем измененную листву из результата и накладываем на сохраненный фон
    new_foliage = img_transformed * mask_3channel
    final_image = (background + new_foliage).astype(np.uint8)

    # 7. Сохранение
    cv2.imwrite(output_path, final_image)
    print(f"Изображение сохранено как {output_path}")


# Основной блок программы
if __name__ == "__main__":
    # Превращаем осень (Photo1) в лето -> Summer.jpg
    transform_season('Photo1.jpg', 'Photo2.jpg', 'Summer.jpg',
                     input_season='autumn', donor_season='summer')

    # Превращаем лето (Photo2) в осень -> Autumn.jpg
    transform_season('Photo2.jpg', 'Photo1.jpg', 'Autumn.jpg',
                     input_season='summer', donor_season='autumn')