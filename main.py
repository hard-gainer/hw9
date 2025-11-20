import uuid
import asyncio
import csv
from typing import Any, List, Optional
from contextlib import asynccontextmanager

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Integer, select, func, distinct, delete, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import UUID

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field


class Base(DeclarativeBase):
    pass


class StudentCreate(BaseModel):
    surname: str
    name: str
    faculty: str
    grade: str
    mark: int = Field(ge=0, le=100)


class StudentUpdate(BaseModel):
    surname: Optional[str] = None
    name: Optional[str] = None
    faculty: Optional[str] = None
    grade: Optional[str] = None
    mark: Optional[int] = Field(None, ge=0, le=100)


class StudentResponse(BaseModel):
    uuid: str
    surname: str
    name: str
    faculty: str
    grade: str
    mark: int

    class Config:
        from_attributes = True


class AverageMarkResponse(BaseModel):
    faculty: str
    average_mark: float


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
        """Создание студента"""
        self.session.add(student)
        await self.session.commit()
        await self.session.refresh(student)
        return student

    async def insert_many(self, students: List[Student]) -> None:
        """Массовое создание студентов"""
        self.session.add_all(students)
        await self.session.commit()

    async def select_all(self) -> List[Student]:
        """Получение всех студентов"""
        result = await self.session.execute(select(Student))
        return list(result.scalars().all())

    async def select_by_uuid(self, student_uuid: uuid.UUID) -> Optional[Student]:
        """Получение студента по UUID"""
        result = await self.session.execute(
            select(Student).where(Student.uuid == student_uuid)
        )
        return result.scalar_one_or_none()

    async def update_student(
        self, student_uuid: uuid.UUID, update_data: dict
    ) -> Optional[Student]:
        """Обновление данных студента"""
        update_data = {k: v for k, v in update_data.items() if v is not None}

        if not update_data:
            return await self.select_by_uuid(student_uuid)

        await self.session.execute(
            update(Student).where(Student.uuid == student_uuid).values(**update_data)
        )
        await self.session.commit()

        return await self.select_by_uuid(student_uuid)

    async def delete_student(self, student_uuid: uuid.UUID) -> bool:
        """Удаление студента"""
        result = await self.session.execute(
            delete(Student).where(Student.uuid == student_uuid)
        )
        await self.session.commit()
        return result.rowcount > 0

    async def select_by_surname(self, surname: str) -> List[Student]:
        """Получение студентов по фамилии"""
        result = await self.session.execute(
            select(Student).where(Student.surname == surname)
        )
        return list(result.scalars().all())

    async def load_from_csv(self, filepath: str) -> None:
        """Загрузка данных из CSV"""
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
        """Получение студентов по факультету"""
        result = await self.session.execute(
            select(Student).where(Student.faculty == faculty)
        )
        return list(result.scalars().all())

    async def get_unique_grades(self) -> List[str]:
        """Получение уникальных курсов"""
        result = await self.session.execute(select(distinct(Student.grade)))
        return list(result.scalars().all())

    async def get_average_mark_by_faculty(self, faculty: str) -> Optional[float]:
        """Получение среднего балла по факультету"""
        result = await self.session.execute(
            select(func.avg(Student.mark)).where(Student.faculty == faculty)
        )
        avg_mark = result.scalar_one_or_none()
        return float(avg_mark) if avg_mark is not None else None

    async def get_students_by_grade_with_low_marks(
        self, grade: str, threshold: int = 30
    ) -> List[Student]:
        """Получение студентов с низкими оценками по курсу"""
        result = await self.session.execute(
            select(Student)
            .where(Student.grade == grade)
            .where(Student.mark < threshold)
        )
        return list(result.scalars().all())


engine = None
async_session_maker = None


async def init_db():
    """Инициализация БД"""
    global engine, async_session_maker

    engine = create_async_engine(
        "postgresql+asyncpg://user:password@localhost:5432/postgres", echo=True
    )

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncSession:
    """Dependency для получения сессии БД"""
    async with async_session_maker() as session:
        yield session


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle для FastAPI"""
    await init_db()
    yield
    if engine:
        await engine.dispose()


app = FastAPI()


@app.post("/students/", response_model=StudentResponse, status_code=201)
async def create_student(
    student_data: StudentCreate, session: AsyncSession = Depends(get_session)
):
    """Создание нового студента"""
    repo = StudentRepository(session)

    student = Student(
        surname=student_data.surname,
        name=student_data.name,
        faculty=student_data.faculty,
        grade=student_data.grade,
        mark=student_data.mark,
    )

    created_student = await repo.insert(student)
    return StudentResponse.model_validate(created_student)


@app.get("/students/", response_model=List[StudentResponse])
async def get_all_students(session: AsyncSession = Depends(get_session)):
    """Получение всех студентов"""
    repo = StudentRepository(session)
    students = await repo.select_all()
    return [StudentResponse.model_validate(s) for s in students]


@app.get("/students/{student_uuid}", response_model=StudentResponse)
async def get_student(
    student_uuid: uuid.UUID, session: AsyncSession = Depends(get_session)
):
    """Получение студента по UUID"""
    repo = StudentRepository(session)
    student = await repo.select_by_uuid(student_uuid)

    if not student:
        raise HTTPException(status_code=404, detail="Student not found")

    return StudentResponse.model_validate(student)


@app.put("/students/{student_uuid}", response_model=StudentResponse)
async def update_student(
    student_uuid: uuid.UUID,
    student_data: StudentUpdate,
    session: AsyncSession = Depends(get_session),
):
    """Обновление данных студента"""
    repo = StudentRepository(session)

    existing_student = await repo.select_by_uuid(student_uuid)
    if not existing_student:
        raise HTTPException(status_code=404, detail="Student not found")

    updated_student = await repo.update_student(
        student_uuid, student_data.model_dump(exclude_unset=True)
    )

    return StudentResponse.model_validate(updated_student)


@app.delete("/students/{student_uuid}", status_code=204)
async def delete_student(
    student_uuid: uuid.UUID, session: AsyncSession = Depends(get_session)
):
    """Удаление студента"""
    repo = StudentRepository(session)

    deleted = await repo.delete_student(student_uuid)

    if not deleted:
        raise HTTPException(status_code=404, detail="Student not found")

    return None


async def main():
    await init_db()

    async with async_session_maker() as session:
        repo = StudentRepository(session)

        await repo.load_from_csv("students.csv")

        all_students = await repo.select_all()
        print(f"Всего студентов: {len(all_students)}\n")

        fpmі_students = await repo.get_students_by_faculty("ФПМИ")
        print(f"Студентов на ФПМИ: {len(fpmі_students)}")

        unique_grades = await repo.get_unique_grades()
        print(f"Уникальные курсы: {unique_grades}\n")

        avg_mark = await repo.get_average_mark_by_faculty("ФПМИ")
        print(f"Средний балл на ФПМИ: {avg_mark:.2f}\n")


if __name__ == "__main__":
    asyncio.run(main())
