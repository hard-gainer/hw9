import uuid
import asyncio
import csv
from typing import Any, List, Optional
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Integer, select, func, distinct
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from sqlalchemy.dialects.postgresql import UUID


class Base(DeclarativeBase):
    pass


class Student(Base):
    __tablename__ = "students"

    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    surname = Column(String, nullable=False)
    name = Column(String, nullable=False)
    faculty = Column(String, nullable=False)
    grade = Column(String, nullable=False)
    mark = Column(Integer, nullable=False)

    def __repr__(self) -> str:
        return f"Student(surname={self.surname}, name={self.name}, faculty={self.faculty}, grade={self.grade}, mark={self.mark})"

    def to_dict(self) -> dict[str, Any]:
        return {
            "uuid": str(self.uuid),
            "surname": self.surname,
            "name": self.name,
            "faculty": self.faculty,
            "grade": self.grade,
            "mark": self.mark,
        }


class StudentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def insert(self, student: Student) -> Student:
        self.session.add(student)
        await self.session.commit()
        await self.session.refresh(student)
        return student

    async def insert_many(self, students: List[Student]) -> None:
        self.session.add_all(students)
        await self.session.commit()

    async def select_all(self) -> List[Student]:
        result = await self.session.execute(select(Student))
        return list(result.scalars().all())

    async def select_by_uuid(self, student_uuid: uuid.UUID) -> Optional[Student]:
        result = await self.session.execute(
            select(Student).where(Student.uuid == student_uuid)
        )
        return result.scalar_one_or_none()

    async def select_by_surname(self, surname: str) -> List[Student]:
        result = await self.session.execute(
            select(Student).where(Student.surname == surname)
        )
        return list(result.scalars().all())

    async def load_from_csv(self, filepath: str) -> None:
        students = []

        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                student = Student(
                    surname=row["Фамилия"],
                    name=row["Имя"],
                    faculty=row["Факультет"],
                    grade=row["Курс"],
                    mark=int(row["Оценка"]),
                )
                students.append(student)

        await self.insert_many(students)

    async def get_students_by_faculty(self, faculty: str) -> List[Student]:
        result = await self.session.execute(
            select(Student).where(Student.faculty == faculty)
        )
        return list(result.scalars().all())

    async def get_unique_grades(self) -> List[str]:
        result = await self.session.execute(select(distinct(Student.grade)))
        return list(result.scalars().all())

    async def get_average_mark_by_faculty(self, faculty: str) -> Optional[float]:
        result = await self.session.execute(
            select(func.avg(Student.mark)).where(Student.faculty == faculty)
        )
        avg_mark = result.scalar_one_or_none()
        return float(avg_mark) if avg_mark is not None else None

    async def get_students_by_grade_with_low_marks(
        self, grade: str, threshold: int = 30
    ) -> List[Student]:
        result = await self.session.execute(
            select(Student)
            .where(Student.grade == grade)
            .where(Student.mark < threshold)
        )
        return list(result.scalars().all())


async def create_db():
    engine = create_async_engine(
        "postgresql+asyncpg://user:password@localhost:5432/postgres"
    )

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    return engine


async def main():
    engine = await create_db()
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        repo = StudentRepository(session)

        await repo.load_from_csv("students.csv")

        all_students = await repo.select_all()
        print(f"Всего студентов: {len(all_students)}\n")

        # Получение списка студентов по факультету
        fpmі_students = await repo.get_students_by_faculty("ФПМИ")
        print(f"Студентов на ФПМИ: {len(fpmі_students)}")
        for student in fpmі_students[:3]:
            print(f"  {student}")
        print()

        # Получение списка уникальных курсов
        unique_grades = await repo.get_unique_grades()
        print(f"Уникальные курсы ({len(unique_grades)}):")
        for grade in unique_grades:
            print(f"  {grade}")
        print()

        # Получение среднего балла по факультету
        avg_mark = await repo.get_average_mark_by_faculty("ФПМИ")
        print(f"Средний балл на ФПМИ: {avg_mark:.2f}\n")

        # Получение студентов по курсу с оценкой ниже 30
        low_marks = await repo.get_students_by_grade_with_low_marks("Мат. Анализ", 30)
        print(f"Студентов с оценкой < 30 по Мат. Анализу: {len(low_marks)}")
        for student in low_marks[:5]:
            print(f"  {student}")


if __name__ == "__main__":
    asyncio.run(main())
