from io import BytesIO

from app.models.domain import Event, URL, User


def test_create_user_persists_to_database(client, db_session) -> None:
    response = client.post(
        "/users",
        json={"username": "silver-user", "email": "silver-user@example.com"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["username"] == "silver-user"
    assert body["email"] == "silver-user@example.com"

    db_user = db_session.query(User).filter(User.email == "silver-user@example.com").first()
    assert db_user is not None
    assert db_user.username == "silver-user"


def test_create_url_records_event_and_can_be_fetched(client, db_session) -> None:
    user = User(username="url-owner", email="url-owner@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    response = client.post(
        "/urls",
        json={
            "user_id": user.id,
            "original_url": "https://example.com/silver",
            "title": "Silver URL",
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["user_id"] == user.id
    assert body["title"] == "Silver URL"
    assert len(body["short_code"]) == 6

    db_url = db_session.query(URL).filter(URL.id == body["id"]).first()
    assert db_url is not None
    assert db_url.original_url == "https://example.com/silver"

    db_event = db_session.query(Event).filter(Event.url_id == db_url.id, Event.event_type == "created").first()
    assert db_event is not None
    assert db_event.details["short_code"] == body["short_code"]

    fetch_response = client.get(f"/urls/{db_url.id}")
    assert fetch_response.status_code == 200
    assert fetch_response.json()["id"] == db_url.id

    accessed_event = db_session.query(Event).filter(Event.url_id == db_url.id, Event.event_type == "accessed").first()
    assert accessed_event is not None


def test_bulk_user_upload_skips_invalid_rows_and_duplicates(client, db_session) -> None:
    db_session.add(User(username="existing-user", email="existing@example.com"))
    db_session.commit()

    csv_content = (
        "username,email\n"
        "valid-user,valid@example.com\n"
        "duplicate-user,existing@example.com\n"
        "broken-user,not-an-email\n"
        ",missing@example.com\n"
    )

    response = client.post(
        "/users/bulk",
        files={"file": ("users.csv", BytesIO(csv_content.encode("utf-8")), "text/csv")},
    )

    assert response.status_code == 200
    assert response.json() == {"count": 1}

    emails = {user.email for user in db_session.query(User).all()}
    assert emails == {"existing@example.com", "valid@example.com"}


def test_bulk_user_upload_skips_duplicates_within_same_file_case_insensitively(client, db_session) -> None:
    csv_content = (
        "username,email\n"
        "valid-user,valid@example.com\n"
        "duplicate-user,VALID@example.com\n"
    )

    response = client.post(
        "/users/bulk",
        files={"file": ("users.csv", BytesIO(csv_content.encode("utf-8")), "text/csv")},
    )

    assert response.status_code == 200
    assert response.json() == {"count": 1}

    users = db_session.query(User).all()
    assert len(users) == 1
    assert users[0].email == "valid@example.com"


def test_bulk_user_upload_allows_seed_style_duplicate_usernames(client, db_session) -> None:
    csv_content = (
        "id,username,email,created_at\n"
        "1,seed-user,seed1@example.com,2025-01-01 00:00:00\n"
        "2,seed-user,seed2@example.com,2025-01-02 00:00:00\n"
    )

    response = client.post(
        "/users/bulk",
        files={"file": ("users.csv", BytesIO(csv_content.encode("utf-8")), "text/csv")},
    )

    assert response.status_code == 200
    assert response.json() == {"count": 2}

    users = db_session.query(User).order_by(User.id).all()
    assert len(users) == 2
    assert users[0].username == "seed-user"
    assert users[1].username == "seed-user"


def test_events_endpoint_returns_history_for_created_and_updated_urls(client, db_session) -> None:
    user = User(username="event-owner", email="event-owner@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    create_response = client.post(
        "/urls",
        json={
            "user_id": user.id,
            "original_url": "https://example.com/events",
            "title": "Event URL",
        },
    )
    url_id = create_response.json()["id"]

    update_response = client.put(
        f"/urls/{url_id}",
        json={"title": "Event URL Updated", "is_active": False},
    )

    assert update_response.status_code == 200
    assert update_response.json()["is_active"] is False

    events_response = client.get("/events")

    assert events_response.status_code == 200
    event_types = [event["event_type"] for event in events_response.json()]
    assert event_types == ["created", "updated"]


def test_inactive_url_returns_not_found_and_does_not_create_access_event(client, db_session) -> None:
    user = User(username="inactive-owner", email="inactive-owner@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    create_response = client.post(
        "/urls",
        json={
            "user_id": user.id,
            "original_url": "https://example.com/inactive",
            "title": "Inactive URL",
        },
    )
    url_id = create_response.json()["id"]

    update_response = client.put(
        f"/urls/{url_id}",
        json={"is_active": False},
    )
    assert update_response.status_code == 200

    fetch_response = client.get(f"/urls/{url_id}")

    assert fetch_response.status_code == 404
    assert fetch_response.json() == {"detail": "URL not found"}

    access_events = db_session.query(Event).filter(Event.url_id == url_id, Event.event_type == "accessed").all()
    assert access_events == []


def test_missing_resource_returns_json_404(client) -> None:
    response = client.get("/users/9999")

    assert response.status_code == 404
    assert response.json() == {"detail": "User not found"}


def test_delete_user_removes_user_and_related_records(client, db_session) -> None:
    user = User(username="delete-user", email="delete-user@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    url = URL(
        user_id=user.id,
        short_code="Del123",
        original_url="https://example.com/delete-user",
        title="Delete User URL",
    )
    db_session.add(url)
    db_session.commit()
    db_session.refresh(url)

    db_session.add(Event(url_id=url.id, user_id=user.id, event_type="created", details={"short_code": url.short_code}))
    db_session.commit()

    response = client.delete(f"/users/{user.id}")

    assert response.status_code == 204
    assert db_session.query(User).filter(User.id == user.id).first() is None
    assert db_session.query(URL).filter(URL.id == url.id).first() is None
    assert db_session.query(Event).filter(Event.user_id == user.id).all() == []


def test_redirect_short_code_returns_redirect_and_records_click(client, db_session) -> None:
    user = User(username="redirect-user", email="redirect-user@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    url = URL(
        user_id=user.id,
        short_code="Go1234",
        original_url="https://example.com/redirect-target",
        title="Redirect Target",
    )
    db_session.add(url)
    db_session.commit()
    db_session.refresh(url)

    response = client.get(f"/urls/{url.short_code}/redirect", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["location"] == "https://example.com/redirect-target"

    click_event = db_session.query(Event).filter(Event.url_id == url.id, Event.event_type == "click").first()
    assert click_event is not None


def test_get_urls_can_filter_by_active_flag(client, db_session) -> None:
    user = User(username="active-user", email="active-user@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    active_url = URL(
        user_id=user.id,
        short_code="Act111",
        original_url="https://example.com/active",
        title="Active URL",
        is_active=True,
    )
    inactive_url = URL(
        user_id=user.id,
        short_code="Ina111",
        original_url="https://example.com/inactive-filter",
        title="Inactive URL",
        is_active=False,
    )
    db_session.add_all([active_url, inactive_url])
    db_session.commit()

    response = client.get("/urls", params={"is_active": "true"})

    assert response.status_code == 200
    body = response.json()
    assert [item["short_code"] for item in body] == ["Act111"]


def test_events_endpoint_supports_filters_and_create_event(client, db_session) -> None:
    user = User(username="filter-user", email="filter-user@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    url = URL(
        user_id=user.id,
        short_code="Evt111",
        original_url="https://example.com/events-filter",
        title="Events Filter URL",
    )
    db_session.add(url)
    db_session.commit()
    db_session.refresh(url)

    create_response = client.post(
        "/events",
        json={
            "url_id": url.id,
            "user_id": user.id,
            "event_type": "click",
            "details": {"referrer": "https://google.com"},
        },
    )

    assert create_response.status_code == 201
    assert create_response.json()["event_type"] == "click"

    response = client.get("/events", params={"url_id": url.id, "user_id": user.id, "event_type": "click"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["event_type"] == "click"
    assert body[0]["url_id"] == url.id
    assert body[0]["user_id"] == user.id


def test_delete_url_removes_url_and_associated_events(client, db_session) -> None:
    user = User(username="delete-url-user", email="delete-url-user@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    url = URL(
        user_id=user.id,
        short_code="Gone11",
        original_url="https://example.com/delete-url",
        title="Delete URL",
    )
    db_session.add(url)
    db_session.commit()
    db_session.refresh(url)

    db_session.add(Event(url_id=url.id, user_id=user.id, event_type="created", details={"short_code": url.short_code}))
    db_session.commit()

    response = client.delete(f"/urls/{url.id}")

    assert response.status_code == 204
    assert db_session.query(URL).filter(URL.id == url.id).first() is None
    assert db_session.query(Event).filter(Event.url_id == url.id).all() == []
