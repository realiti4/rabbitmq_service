using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using background_service.Models;

// Auth
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Authorization;

namespace background_service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TodoItemsController : ControllerBase
    {
        private readonly TodoContext _context;

        public TodoItemsController(TodoContext context)
        {
            _context = context;
        }

        // GET: api/TodoItems
        [HttpGet]
        public async Task<ActionResult<IEnumerable<TodoItem>>> GetTodoItems()
        {
            return await _context.TodoItems.ToListAsync();
        }

        // GET: api/TodoItems/5
        [HttpGet("{id}")]
        public async Task<ActionResult<TodoItem>> GetTodoItem(long id)
        {
            var todoItem = await _context.TodoItems.FindAsync(id);

            if (todoItem == null)
            {
                return NotFound();
            }

            return todoItem;
        }

        // PUT: api/TodoItems/5
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPut("{id}")]
        public async Task<IActionResult> PutTodoItem(long id, TodoItem todoItem)
        {
            if (id != todoItem.Id)
            {
                return BadRequest();
            }

            _context.Entry(todoItem).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!TodoItemExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }

            return NoContent();
        }

        // POST: api/TodoItems
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPost]
        public async Task<ActionResult<TodoItem>> PostTodoItem(TodoItem todoItem)
        {
            _context.TodoItems.Add(todoItem);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetTodoItem", new { id = todoItem.Id }, todoItem);
        }

        // DELETE: api/TodoItems/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteTodoItem(long id)
        {
            var todoItem = await _context.TodoItems.FindAsync(id);
            if (todoItem == null)
            {
                return NotFound();
            }

            _context.TodoItems.Remove(todoItem);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        [Authorize]
        [HttpGet("dev")]
        public void Dev()
        {
            List<int> test = new List<int>() { 1, 2, 3, 4, 5 };

            IEnumerable<int> numberList = Enumerable.Range(1, 10);

            var test3 = numberList.Where(x => x % 2 == 0)
                .Select(x => x * 2);

            var test2 = test.Where(x => x >= 4)
                .Select(x => x * 2);

            foreach (var item in test2)
            {
                Console.WriteLine(item);
            }
        }

        private bool TodoItemExists(long id)
        {
            return _context.TodoItems.Any(e => e.Id == id);
        }
    }

    // Auth controller
    [Route("api/[controller]")]
    [ApiController]
    public class AuthController : ControllerBase
    {
        private readonly IConfiguration _config;
        private readonly Int32 _token_expiration = 30;

        public AuthController(IConfiguration config)
        {
            _config = config;
        }

        [HttpPost("login")]
        public async Task<IActionResult> Login(string username, string password)
        {
            // Validate user
            if (is_valid_user(username, password))
            {
                // Create token
                var token = generate_token(username);

                return Ok(new { token });
            }

            return Unauthorized();
        }

        private bool is_valid_user(string username, string password)
        {
            return username == "admin" && password == "admin";
        }

        private string generate_token(string username)
        {
            var security_key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_config["Jwt:Key"]));
            var credentials = new SigningCredentials(security_key, SecurityAlgorithms.HmacSha256);

            var claims = new[]
            {
                new Claim(ClaimTypes.Name, username)
            };

            var token = new JwtSecurityToken(
                issuer: _config["Jwt:Issuer"],
                audience: _config["Jwt:Audience"],
                claims: claims,
                expires: DateTime.Now.AddMinutes(_token_expiration),
                signingCredentials: credentials
            );

            return new JwtSecurityTokenHandler().WriteToken(token);
        }
    }
}
