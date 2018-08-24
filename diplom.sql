
-- Создаем таблички

-- content_info
create table if not exists content_info(
docUuid varchar(200),
visitors integer,
date date,
isLong integer,
timeReadExpected integer);

-- social_all
create table if not exists social_all(
docUuid varchar(200),
visits integer,
shareTotal integer,
visits_social integer,
social_R integer,
social_V integer,
social_S3 integer);

-- search_all
create table if not exists search_all(
docUuid varchar(200),
visits integer,
desktop_search_R integer,
desktop_search_V integer,
desktop_search_S3 integer);

-- all_desktop
create table if not exists all_desktop(
docUuid varchar(200),
visits integer,
desktop_all_R integer,
desktop_all_V integer,
desktop_all_S3 integer);

-- content_origin - ПРОБЛЕМА С КЛЮЧАМИ ПЕРЕДЕЛАТЬ ФАЙЛ
create table if not exists content_origin(
docUuid varchar(200) primary key,
isLong integer,
timeReadExpected integer);



-- копируем данные
\copy content_info from '/data/id_info.csv' DELIMITER ',' CSV HEADER
\copy social_all from '/data/sharings.csv' DELIMITER ',' CSV HEADER
\copy all_desktop from '/data/all_desktop.csv' DELIMITER ',' CSV HEADER
\copy search_all from '/data/searching.csv' DELIMITER ',' CSV HEADER


-- подключимся к базе и проверим, все ли выгрузилось

psql --host $APP_POSTGRES_HOST -U postgres
select * from content_info limit 3;
select * from social_all limit 3;
select * from search_all limit 3;
select * from all_desktop limit 3;

-- ЗАПРОСЫ
--  #1
-- Выводим топ-5 материалов, где лучший доскролл. Для этого сравниваем визиты на десктопе и значение desktop_all_s3.
-- Чем меньше разница - тем лучше доскролл. Также мы откидываем материалы с небольшим трафиком
-- В данном случае не группируем, так как нас устраивает вариант иза один день
select docuuid, desktop_all_v, desktop_all_s3, (desktop_all_v/desktop_all_s3)
as good_readers from all_desktop
where desktop_all_s3<>0 and visits > 5000 order by good_readers asc limit 5;

-- #2
-- Посчитаем сколько всего материалов с переходами из соцсетей больше 1000
select count(docuuid) from social_all where visits_social>1000;
-- Вывести максимальное количество переходов из поиска
select max(desktop_search_v) from search_all;
-- Выведем минимальное количество пользователей, которые стали читателями на десктопной версии сайта
-- Сделаем это только для материалов, на которых более 1000 посетителей
select docuuid, min(desktop_all_r) from all_desktop where visits>1000 group by docuuid limit 5;

-- #4
-- Выводим топ-5 материалов по посетителям за определенный день
select docuuid, visitors, date from content_info where date = '2018-08-21'
group by docuuid, visitors, date
order by visitors desc limit 10;

-- #5
-- смотрим примерное время чтения в материалах с хорошим доскроллом.
-- Для этого используем join с таблице content_info и нам придется ипользовать группировку, тк id не уникальные
select all_desktop.docuuid, all_desktop.desktop_all_v, content_info.timereadexpected,
(all_desktop.desktop_all_v/all_desktop.desktop_all_s3) as good_readers
from all_desktop inner join content_info on content_info.docuuid = all_desktop.docuuid
where all_desktop.desktop_all_s3<>0 and all_desktop.visits > 3000
group by all_desktop.docuuid, all_desktop.desktop_all_v, content_info.timereadexpected, all_desktop.desktop_all_s3
order by good_readers asc limit 5;

-- #6 - аналитическая функция
-- Выводим количество действий с шарами (не просто сколько раз расшаривали, а фактически сколько дней шарили)
-- Сортируем действия накопленным итогом
-- В sample берем только те материалы, где читателями стали более ста посетителей
select docuuid, sharetotal, visits_social, count(visits_social)
over(partition by docuuid order by visits_social asc)
as count_rate
from(select docuuid, visits, sharetotal, visits_social from social_all where social_r >100 order by docuuid)
as sample order by count_rate asc;

-- #7 - аналитическая функция
-- Смотрим разницу среднего поискового трафика и поискового трафика за каждый день
-- Данный запрос позволяет увидеть сколько дней было трафика на каждом материале и какая разница между максимальным и минимальным трафиком.
-- А также мы сможем увидеть статьи с нулевым отклонением. Тогда трафик будет только один день и это странно
select docuuid, desktop_search_v,
(max(desktop_search_v) over(partition by docuuid) - avg(desktop_search_v)
over(partition by docuuid)) as traffic_dif
from(select docuuid, visits, desktop_search_v from search_all
where visits >=1000 order by docuuid desc)
as sample order by traffic_dif desc limit 10;


-- #8
-- Смотрим поисковый трафик на длинные материалы и также выводим среднее время чтения
select distinct content_info.docuuid, content_info.timereadexpected, search_all.desktop_search_v
from content_info
join search_all on search_all.docuuid = content_info.docuuid
where content_info.islong = 1
order by search_all.desktop_search_v desc limit 10;

-- #9 - аналитическая функция
-- Смотрим количество посетителей на длинных статьях (islong = 1) за два дня - 20 и 21 августа
-- Нумеруем материалы с помощью row_number()
-- Благодаря запросы нам удобно посмотреть визиты по материалам с конкретными характеристиками
select docuuid, visitors,
row_number() over (partition by docuuid) as counter_visit
from(select docuuid, visitors, islong from content_info where islong = 1 and date > '2018-08-19' ) as sample
order by counter_visit asc limit 25;

-- #10 аналитическая функция
--  Выводим материалы, до которых скроллили хуже всего
--  Делаем условие, что это только материалы, где читателями стали более тысячи или менее пяти тысяч
select docuuid, desktop_all_v, desktop_all_s3, max(desktop_all_v/desktop_all_s3) over (partition by docuuid)
as min_scroll
from(select docuuid, desktop_all_v, desktop_all_s3 from all_desktop
where desktop_all_r > 1000 and desktop_all_r < 5000) as sample
order by min_scroll desc limit 10;

-- #11 аналитическая функция
-- Усложняем задачу и выводим приогнозируемое время чтение из другой таблицы (timereadexpected)
-- Благодаря запросу мы можем увидеть, материалы какого объема читают хуже всего
select docuuid, timereadexpected, desktop_all_v, desktop_all_s3,
max(desktop_all_v/desktop_all_s3)
over (partition by docuuid) as min_scroll
from(select all_desktop.docuuid, content_info.timereadexpected, all_desktop.desktop_all_v, all_desktop.desktop_all_s3 from all_desktop
join content_info on all_desktop.docuuid = content_info.docuuid
where  all_desktop.desktop_all_r > 1000) as sample
order by min_scroll desc limit 10;

-- ПРЕДСТАВЛЕНИЯ

-- Топ-50 материалов по шарам
-- фильтр: в выборку не входят материалы, на которые не переходили из соцсетей (то есть только шарили)
create view best_sharing as
  select distinct docuuid, sharetotal from social_all
  where visits_social <> 0 order by sharetotal desc limit 50;

-- Топ материалов по поисковому трафику и трафику из соцсетей
-- Сортируем по поисковому трафику
create view best_search_social4 as
  select distinct search_all.docUuid, sum(search_all.desktop_search_v) as best_search,
  sum(social_all.social_v) as best_social
  from search_all inner join social_all on search_all.docUuid = social_all.docUuid
  group by search_all.docUuid
  order by best_search desc limit 10;
